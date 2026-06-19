"""
Peer_connection - Responsável por fazer a conexão entre peers e a coordenação de mensagens
Tasks intermitentes: Esperar por novas conexões INBOUND e faz conexões OUTBOUND pedidas
                     além de, enviar respostas para mensagens enviadas por outros peers

Classe: PeerConnection
    Funções públicas:
        Start(my_name, my_namespace)
            →Começa o processo de ouvir por novas conexões

        Connect_Out(peer_id, ip, port)
            →Cria uma conexão OUTBOUND com o ip/port passado, só deve ser chamado após verificar 
            que não há conexão INBOUND desse peer na tabela, ele mesmo atualisa a tabela peer 
            depois que a conexão é confirmada
        
        Full_disconnect()
            →Serve para desligar tudo, mandando mensagens de BYE para todos os peers e um por 
            um os retira da peer table antes de desligar completament
        
        Sender(msg, peer)
            → Manda a mensagem (dicionario sem ser traduzido para json) para o peer (peer_id) desejado
            → No caso de keep_alive e ao mandar mensagem que se quer um ACK, deve-se colocar antes de 
            chamar o sender a função do state.py de add_pending_ack ou add_pending_ping, note que, o 
            remove_pending é tratado no _receiver_handler, e o ts de ping e pong podem ser vistos no ts_info

    Outras funções:
        _first_contact
            → Responsável por ouvir qualquer um tentando estabelecer uma conexão

        _disconnect_inbound
            → Trata de desconectar peers que mandaram "BYE" utilizando protocolo adequado

        _receiver_handler
            → Função criada para cada peer conectado, resposável por ouvir qualquer mensagem que 
            esse passe, dando uma resposta adequada quando necessário
    
"""

import json
import time
import socket
import logging
import threading
from uuid import uuid4
from dataclasses import dataclass
from datetime import datetime, timezone

from state import State

@dataclass 
class ts_info:
    _ts_last_ping: float = None
    _ts_last_pong: float = None

    def __post_init__(self):
        self._ping_lock = threading.Lock()
        self._pong_lock = threading.Lock()

    def change_last_ping(self, new_ts):
        with self._ping_lock:
            self._ts_last_ping = new_ts

    def change_last_pong(self, new_ts):
        with self._pong_lock:
            self._ts_last_pong = new_ts

    def get_last_ping(self):
        with self._ping_lock:
            return self._ts_last_ping

    def get_last_pong(self):
        with self._pong_lock:
            return self._ts_last_pong


class PeerConnection:
    def __init__(self, my_ip: str, my_port: int, features:list, peer_states: State =None, logs: logging.Logger = None):
        self.my_ip = my_ip
        self.my_port = my_port
        self.peer_states = peer_states
        self.features = features
        self.log = logs

        self.my_peer_id = None
        self.peer_ttl = None

        self.infos = {} #para se colocar peer junto das informações de tempo
        self.infos_lock = threading.Lock()
        self.threads_ativas = {}
        self.thread_ativas_lock = threading.Lock()

        self.sock_ouvinte: socket.socket = None
        self.senders_locks = {}
        self.senders_locks_lock = threading.Lock() #porque alterar o dicionários de locks pode causar erro
        self.listening = threading.Event()
        

    def Start(self,my_name: str, my_namespace: str, peer_ttl: int = 1):
        self.my_peer_id = my_name + "@" + my_namespace
        self.peer_ttl = peer_ttl
        try:
            self.log.info(f"[Peer_connection] Starting")
            self.sock_ouvinte = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock_ouvinte.bind((self.my_ip,self.my_port))
            self.log.debug(f"[Peer_connection] Ouvindo mensagens")
            self.listening.set()
            ouvinte = threading.Thread(target=self._first_contact)
            ouvinte.start()
        except Exception as error:
            self.log.warning(f"[Peer_connection] Erro ao começar, {error}")


    #Primeiro contato com peers que tentam se conectar ao servidor
    def _first_contact(self):
        sock = self.sock_ouvinte
        sock.listen()
        while (self.listening.is_set()): #Estará esperando conexões nessa porta até se encerrar
            peer_socket = None
            try:
                peer_socket, addr = sock.accept() #Começa a ouvir na sock iniciada em Start
                received = peer_socket.recv(1024).decode()
                received = json.loads(received)

                if received.get('type') != "HELLO" or 'peer_id' not in received: #Verificação se a mensagem recebida é um Hello com a id
                    self.log.debug(f"[Peer_connection] Protocolo de handshake impróprio")
                    peer_socket.close()
                    continue


                self.log.debug(f"[Peer_connection] Conexão estabelecida com {received['peer_id']}")

                #coletando informações e guardando elas
                id = received['peer_id']
                self.peer_states.update_peer(id, addr[0], addr[1]) #updates the state table
                self.peer_states.add_connection(id, peer_socket,"INBOUND") #passa o socket e direção para a state table
                with self.senders_locks_lock:
                    self.senders_locks[id] = threading.Lock()
                with self.infos_lock:
                    self.infos[id] = ts_info()

                #respondendo
                resposta = {"type":"HELLO_OK","peer_id":self.my_peer_id,"version":"1.0","features":self.features,"ttl":self.peer_ttl}
                self.Sender(resposta, id)

                #iniciando a porta de escuta para o que esse peer enviar
                tr = threading.Thread(target=self._receiver_handler, args=[id]) #Começa o processo de ouvir o que o peer enviar
                tr.start()
                with self.thread_ativas_lock:
                    self.threads_ativas[id] = tr

            except Exception as e:
                self.log.warning(f"[Peer_connection] Erro: {e}")
                try:
                    if peer_socket is not None:
                        try:
                            peer_socket.close()
                        except Exception:
                            pass
                except:
                    pass

    #Chamado para fazer a conexão com um peer descoberto (Outbound)
    def Connect_Out(self, peer_id, ip, port):
        self.log.debug(f"[Peer_connection] Se conectando a {peer_id}")
        lock_created = False
        connection_added = False
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip , port))
            msg = {"type":"HELLO","peer_id":self.my_peer_id,"version":"1.0","features":self.features,"ttl":self.peer_ttl}
            msg = json.dumps(msg) + "\n"
            sock.send(msg.encode())
            sock.settimeout(5.0)
            resposta = json.loads(sock.recv(1024).decode())
            sock.settimeout(None)
            tipo = resposta.get('type')

            if tipo == "HELLO_OK": #conexão aceita, adiciona as informações nos states
                with self.infos_lock:
                    self.infos[peer_id] = ts_info()
                with self.senders_locks_lock:
                    self.senders_locks[peer_id] = threading.Lock()
                    lock_created = True
                self.peer_states.add_connection(peer_id, sock, "OUTBOUND")
                connection_added = True
                tr = threading.Thread(target=self._receiver_handler, args=[peer_id]) #Começa o processo de ouvir, igual ao do First_contact
                tr.start()
                with self.thread_ativas_lock:
                    self.threads_ativas[peer_id] = tr

            else:
                self.log.debug(f"[Peer_connection] Falha ao se conectar com {peer_id}")
                with self.senders_locks_lock:
                    self.senders_locks.pop(peer_id, None)
                self.peer_states.remove_connection(peer_id)
                self.peer_states.register_failed_attempt(peer_id)

        except Exception as error:
            self.log.debug(f"[Peer_connection] Falha ao se conectar com {peer_id} devido a {error}")
            if lock_created:
                with self.senders_locks_lock:
                    self.senders_locks.pop(peer_id, None)
            if connection_added:
                self.peer_states.remove_connection(peer_id)
            self.peer_states.register_failed_attempt(peer_id)
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

    #Chamada quando um peer pede para se desconectar    
    def _disconnect_inbound(self,msg, peer): 
        Final_words = msg
        with self.thread_ativas_lock:
            self.threads_ativas.pop(peer)
        sock = self.peer_states.get_connection(peer)

        msg_funeral = {'type':"BYE_OK", 'msg_id':Final_words.get("uuid"), 
                       'src':Final_words.get("dst"), 'dst':Final_words.get("src"),
                       'ttl':self.peer_ttl}
        
        self.Sender(msg_funeral, peer)
        sock.close()
        self.peer_states.remove_connection(peer)
        with self.senders_locks_lock:
            self.senders_locks.pop(peer, None)
        self.peer_states.remove_peer(peer)
        with self.infos_lock:
            self.infos.pop(peer, None)
        self.log.debug(f"[Peer_connection] Conexão com {peer} encerrada")

    #Disconecta de todos os peers possíveis
    def Full_disconnect(self):
        self.log.info(f"[Peer_connection] Começando processo de encerramento")
        self.listening.clear()
        with self.thread_ativas_lock:
            for j in list(self.threads_ativas.keys()): #garante o encerramento de cada thread
                self.threads_ativas[j].join()
                self.threads_ativas.pop(j)
        my_id = self.my_peer_id

        for i in self.peer_states.get_all_peers():
            self.log.debug(f"[Peer_connection] Encerrando conexão com {i}")
            sock = self.peer_states.get_connection(i)

            try:
                specific_uuid = str(uuid4())
                msg = {'type':"BYE", 'uuid':specific_uuid, 'src': my_id, 'dst': i, 'reason': "Closing all connections", 'ttl': self.peer_ttl}
                self.Sender(msg, i)
                sock.settimeout(5.0)
                retorno = json.loads(sock.recv(32768).decode())

                if (retorno.get('type') == "BYE_OK") and (retorno.get('uuid') == specific_uuid):
                    self.log.debug(f"[Peer_connection] BYE_OK recebido de {i}")
                    self.peer_states.remove_connection(i)
                    sock.close()
                    self.peer_states.remove_peer(i)
                    with self.senders_locks_lock:
                        self.senders_locks.pop(i, None)
                    with self.infos_lock:
                        self.infos.pop(i, None)

                else:
                    self.log.debug(f"[Peer_connection] Falha ao receber BYE_OK correto, encerrando memso assim")
                    self.peer_states.remove_connection(i)
                    sock.close()
                    self.peer_states.remove_peer(i)
                    with self.senders_locks_lock:
                        self.senders_locks.pop(i, None)
                    with self.infos_lock:
                        self.infos.pop(i, None)

            except Exception as e:
                self.log.warning(f"[Peer_connection] Erro: {e}, desconectando mesmo assim")
                sock.close()
                self.peer_states.remove_peer(i)
                with self.senders_locks_lock:
                    self.senders_locks.pop(i, None)
                with self.infos_lock:
                    self.infos.pop(i, None)
        return True
    
    #Escuta cada peer conectado e prepara a resposta necess-aria
    def _receiver_handler(self, peer):
        connected_scokect: socket = self.peer_states.get_connection(peer)
        connected_scokect.settimeout(2) #Para que a cada 2 segundos o thread não trave em recv
        while (self.listening.is_set()):
            try:
                if self.listening.is_set():
                    try:
                        recebido = connected_scokect.recv(32768) #tamanho máximo de mensagens
                        recebido = json.loads(recebido.decode())
                        tempo = datetime.now(timezone.utc).isoformat()
                        tipo = recebido.get('type')
                    except:
                        continue

                    if tipo == "PING":
                        msg = {'type':"PONG", 'msg_id':recebido.get("uuid"), "timestamp":tempo, "ttl":self.peer_ttl}
                        self.Sender(msg, peer)

                    elif tipo == "SEND":
                        self.log.info(f"[MSG] {peer}: {recebido.get('payload')}")
                        if recebido.get('require_ack'):
                            msg = {'type':"ACK", 'msg_id':recebido.get("uuid"), "timestamp":tempo, "ttl":self.peer_ttl}
                            self.Sender(msg, peer)

                    elif tipo == "PUB":
                        self.log.info(f"[MSG] {peer}: {recebido.get('payload')}")
                        if recebido.get('require_ack'):
                            msg = {'type':"ACK", 'msg_id':recebido.get("uuid"), "timestamp":tempo, "ttl":self.peer_ttl}
                            self.Sender(msg, peer)

                    elif tipo == "BYE":
                        self.log.info(f"[Peer_connection] Pedido de fechamento de conexão de {peer}, devido a {recebido.get('reason')}")
                        self._disconnect_inbound(recebido, peer)
                        break #Finalisa a thread, visto que, o self.listening.is_set() fecharia todas as conexões

                    elif tipo == "PONG":
                        self.peer_states.remove_pending_ping(recebido.get("uuid"))
                        with self.infos_lock:
                            info = self.infos.get(peer)
                            if info:
                                info.change_last_pong(time.monotonic())
                                self.log.debug(f"[Peer_connection] PONG recebido de {peer}")
                            else:
                                self.log.warning(f"[Peer_connection] PONG de peer sem info registrada: {peer}")


                    elif tipo == "ACK":
                        self.peer_states.remove_pending_ack(recebido.get("uuid"))
                        self.log.debug(f"[Peer_connection] ACK recebido de {peer}")

                    else:
                        self.log.info(f"[Peer_connection] Mensagem mal formatada de {peer}")
                    

            except Exception as e:
                self.log.warning(f"[Peer_connection] Erro: {e}")
                self.log.debug(f"[Peer_connection] Fechando conexão com {peer}")
                self.peer_states.remove_connection(peer)
                connected_scokect.close()
                with self.senders_locks_lock:
                    self.senders_locks.pop(peer, None)
                self.peer_states.remove_peer(peer)
                with self.infos_lock:
                    self.infos.pop(peer, None)
                break


    #Responsável por enviar as mensagens para os peers
    #Funções que esperam o Retorno de ACK ou um PONG precisam usar o add_pending_... do state.py antes
    def Sender(self, msg_in: dict, peer_id: str):
        msg = json.dumps(msg_in) + "\n"
        with self.senders_locks_lock:
            lock = self.senders_locks.get(peer_id)
        if lock is None:
            self.log.warning(f"[Peer_connection] Erro ao obter o lock ao enviar para {peer_id}")
            return
        
        with lock:
            try:
                sock = self.peer_states.get_connection(peer_id)
                if sock:
                    sock.send(msg.encode())
                    self.log.debug(f"[Peer_connection] Mensagem enviada para {peer_id}")
                else:
                    self.log.warning(f"[Peer_connection] Não há socket para {peer_id}")
            except:
                self.log.warning(f"[Peer_connection] Falha ao mandar msg para: {peer_id}")