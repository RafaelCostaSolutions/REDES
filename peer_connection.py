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

            Obs: Peer_id = name@namespace
        
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
from datetime import datetime, timezone

from state import State


class PeerConnection:
    def __init__(self, my_ip: str, my_port: int, features:list, states: State, logs: logging.Logger = None):
        self.my_ip = my_ip
        self.my_port = my_port
        self.peer_states = states
        self.features = features
        self.log = logs

        self.my_peer_id = None
        self.peer_ttl = None

        self.thread_ativas_lock = threading.Lock()
        self.threads_ativas = {}

        self.sock_ouvinte: socket.socket = None
        self.senders_locks = {}

        self.senders_locks_lock = threading.Lock() #porque alterar o dicionários de locks pode causar erro
        self.listening = threading.Event()
        
    #Inicio do server de peer_connection
    def Start(self,my_name: str, my_namespace: str, peer_ttl: int = 1):
        self.my_peer_id = my_name + "@" + my_namespace
        self.peer_ttl = peer_ttl

        try:
            self.log.info(f"[Peer_connection] Starting")
            self.sock_ouvinte = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            #Fazendo o binfing da socket com o ip e port do próprio aplicativo
            self.sock_ouvinte.bind((self.my_ip,self.my_port))
            self.log.debug(f"[Peer_connection] Listening for connections")

            #Lock para o processo de ouvir continuar até que se seja desligado
            self.listening.set()

            #Começando thread do first contact
            ouvinte = threading.Thread(target=self._first_contact)
            ouvinte.start()

        except Exception as error:
            self.log.warning(f"[Peer_connection] Error on startup: {error}")


    #Primeiro contato com peers que tentam se conectar ao servidor
    def _first_contact(self):
        sock = self.sock_ouvinte
        sock.listen() #Estará esperando conexões nessa porta até se encerrar

        while (self.listening.is_set()):
            peer_socket = None #É necessário uma nova socket para cada conexão
            try:
                #Ao receber um request guarda  socket e o address de quem se conectou
                peer_socket, addr = sock.accept()

                #Devido ao protocolo é necessário um aperto de mãos
                peer_socket.settimeout(5.0) #Espera 5 segundos pelo HELLO
                received = peer_socket.recv(1024).decode()
                received = json.loads(received) #Caso receba None ou obtenha algum outro erro o exception irá cuidar


                #Verificação se a mensagem recebida é um Hello com uma id
                if received.get('type') != "HELLO" or 'peer_id' not in received:
                    self.log.debug(f"[Peer_connection] Handshake protocoll wrong")
                    peer_socket.close()
                    continue
                
                #Verificação se o peer já não está conectado
                id = received.get('peer_id')
                if id in self.peer_states.get_all_connections().keys():
                    self.log.debug(f"[Peer_connection] Peer is already connected")
                    peer_socket.close()
                    continue
                    
                self.log.debug(f"[Peer_connection] Connection stabilished with {id}")

                #coletando informações e guardando elas
                self.peer_states.update_peer(id, addr[0], addr[1]) #updates the state table
                self.peer_states.add_connection(id, peer_socket,"INBOUND") #passa o socket e direção para a state table
                with self.senders_locks_lock:
                    self.senders_locks[id] = threading.Lock()

                #respondendo
                resposta = {"type":"HELLO_OK","peer_id":self.my_peer_id,"version":"1.0","features":self.features,"ttl":self.peer_ttl}
                self.Sender(resposta, id)

                #iniciando a porta de escuta para o que esse peer enviar
                tr = threading.Thread(target=self._receiver_handler, args=[id]) #Começa o processo de ouvir o que o peer enviar
                tr.start()
                with self.thread_ativas_lock:
                    self.threads_ativas[id] = tr

            except OSError:
                if not self.listening.is_set():
                    # socket fechado intencionalmente pelo Full_disconnect
                    self.log.debug(f"[Peer_connection] Listener closed intentionally")

                else:
                    # listening ainda ativo, erro não esperado
                    self.log.warning(f"[Peer_connection] Unexpected error on listening socket")

                break #sem o sock_ouvinte precisa fechar em ambos casos
            
            #Diferenciação de erro de um hello mal formado, para erros 
            except json.JSONDecodeError:
                # mensagem malformada, não é necessário parar o processo
                self.log.debug(f"[Peer_connection] Malformed HELLO received")
                if peer_socket is not None:
                    try:
                        peer_socket.close()
                    except Exception:
                        pass
            
            except Exception as e:
                # erros gerais ao se conectar com um peer, também não deve parar o processo
                self.log.warning(f"[Peer_connection] Error during handshake: {e}")
                if peer_socket is not None:
                    try:
                        peer_socket.close()
                    except Exception:
                        pass

        self.log.debug(f"[Peer_connection] Stoping listening process")        



    #Chamado para fazer a conexão com um peer descoberto (Outbound)
    def Connect_Out(self, peer_id, ip, port):
        self.log.debug(f"[Peer_connection] Connecting to {peer_id}")
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
                self.log.debug(f"[Peer_connection] Failed to connect with {peer_id}")
                with self.senders_locks_lock:
                    self.senders_locks.pop(peer_id, None)
                self.peer_states.remove_connection(peer_id)
                self.peer_states.register_failed_attempt(peer_id)

        except Exception as error:
            self.log.debug(f"[Peer_connection] Failed to connect to {peer_id} reson: {error}")
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

        sock = self.peer_states.get_connection(peer)

        msg_funeral = {'type':"BYE_OK", 'msg_id':Final_words.get('msg_id'), 
                       'src':Final_words.get("dst"), 'dst':Final_words.get("src"),
                       'ttl':self.peer_ttl}
        
        self.Sender(msg_funeral, peer)
        self.peer_states.remove_connection(peer)

        with self.thread_ativas_lock:
            self.threads_ativas.pop(peer, None)

        with self.senders_locks_lock:
            self.senders_locks.pop(peer, None)

        sock.close()
        self.peer_states.remove_peer(peer)
        self.log.debug(f"[Peer_connection] Connection with {peer} ended")


    #Disconecta de todos os peers possíveis
    def Full_disconnect(self):
        self.log.info(f"[Peer_connection] Ending process")


        self.listening.clear()
        if self.sock_ouvinte:  
            try:
                self.sock_ouvinte.close()
            except Exception:
                pass

        thread_ativas_list = []

        with self.thread_ativas_lock: #adquire cada thread, para não causar erros com lock no join
            thread_ativas_list = list(self.threads_ativas.values())
            self.threads_ativas.clear()

        for j in thread_ativas_list: #garante o encerramento de cada thread
                j.join(timeout=1.0)

        my_id = self.my_peer_id

        for i in self.peer_states.get_all_peers():
            self.log.debug(f"[Peer_connection] Ending connection with {i}")
            sock = self.peer_states.get_connection(i)
            if sock is None: #proteção para o caso da sock ter um erro grave ou ser desligada antes do procedimento
                self.peer_states.remove_peer(i)
                continue

            try:
                specific_uuid = str(uuid4())
                msg = {'type':"BYE", 'msg_id':specific_uuid, 'src': my_id, 'dst': i, 'reason': "Closing all connections", 'ttl': self.peer_ttl}
                self.Sender(msg, i)
                sock.settimeout(5.0)
                retorno = json.loads(sock.recv(32768).decode())
                
                #como em ambos esses casos se deve fechar a conexão, sera usado um finally fora para que não seja necessário repetir
                if (retorno.get('type') == "BYE_OK") and (retorno.get('msg_id') == specific_uuid):
                    self.log.debug(f"[Peer_connection] BYE_OK received from {i}")

                else:
                    self.log.debug(f"[Peer_connection] Dind't received BYE_OK, ending connection anyway")


            except Exception as e:
                self.log.warning(f"[Peer_connection] Erro: {e}, ending connection anyway")
            
            #deve-se limpar tudo independente de o que aconteceu
            finally:

                #Os logs principais já foram feitos na parte anterior ao finally
                try:
                    sock.close()
                except Exception:
                    pass
                self.peer_states.remove_connection(i)
                self.peer_states.remove_peer(i)
                with self.senders_locks_lock:
                    self.senders_locks.pop(i, None)
        return True
    
    #Escuta cada peer conectado e prepara a resposta necess-aria
    def _receiver_handler(self, peer):
        connected_scokect: socket = self.peer_states.get_connection(peer)
        connected_scokect.settimeout(2) #Para que a cada 2 segundos o thread não trave
        max_msg = 32768 #tamanho máximo de mensagens

        #Usado para ler mensagens e fazer o buffer delas até se obter o \n 
        #(demorou para lembrar que o TCP nãp garante a chegada completa da mensagem)
        reader_conn = connected_scokect.makefile('rb')

        while (self.listening.is_set()):
            try:
                try:
                    recebido = reader_conn.readline()

                    #tratamento de tamanho da mensagem
                    if len(recebido) > max_msg:
                        self.log.debug(f"[MSG] {peer}: Exceeded the limit of characters")
                        continue

                    recebido = json.loads(recebido.decode())
                    tempo = datetime.now(timezone.utc).isoformat()
                    tipo = recebido.get('type')

                #no caso de um timeout só se quer que o loop volte do começo, outros erros caem para o outro except
                except socket.timeout:
                    continue

                if tipo == "PING":
                    msg = {'type':"PONG", 'msg_id':recebido.get('msg_id'), "timestamp":tempo, "ttl":self.peer_ttl}
                    self.Sender(msg, peer)

                elif tipo == "SEND":
                    self.log.info(f"[MSG] {peer}: {recebido.get('payload')}") #Mostra a informação da mensagem

                    if recebido.get('require_ack'): #Encaminha o ack caso seja especificado
                        msg = {'type':"ACK", 'msg_id':recebido.get('msg_id'), "timestamp":tempo, "ttl":self.peer_ttl}
                        self.Sender(msg, peer)

                #funcionamento de receber um PUB e sua resposta (ACK) é identico ao do SEND
                elif tipo == "PUB":
                    self.log.info(f"[MSG] {peer}: {recebido.get('payload')}")
                    if recebido.get('require_ack'):
                        msg = {'type':"ACK", 'msg_id':recebido.get('msg_id'), "timestamp":tempo, "ttl":self.peer_ttl}
                        self.Sender(msg, peer)

                elif tipo == "BYE":
                    self.log.debug(f"[Peer_connection] Asked to close connection from {peer}, reason: {recebido.get('reason')}")
                    self._disconnect_inbound(recebido, peer)
                    break #Finalisa a thread, visto que, o self.listening.is_set() fecharia todas as conexões
                
                elif tipo == "PONG":
                    uuid = recebido.get('msg_id')
                    pong_received = time.monotonic()
                    ping_sent = self.peer_states.get_pending_ping_time(uuid) #Mudar para a função que retorna o tempo do uuid caso tenha

                    if ping_sent is None:
                        self.log.debug(f"[Peer_connection] PONG without matching PING or with malformation")

                    else:
                        rtt = (pong_received - ping_sent) * 1000
                        self.peer_states.set_rtt(peer, rtt)
                        self.log.debug(f"[Peer_connection] PONG received from: {peer}")
                        self.peer_states.remove_pending_ping(uuid)


                elif tipo == "ACK":
                    self.peer_states.remove_pending_ack(recebido.get('msg_id'))
                    self.log.debug(f"[Peer_connection] ACK received from {peer}")

                else:
                    self.log.debug(f"[Peer_connection] Message poorly formated from: {peer}")
                    
            except OSError as error:
                if (self.listening.is_set()): #erro de socket mesmo sem encerrar o processo
                    self.log.warning(f"[Peer_connection] Erro: {error}")
                    self.log.debug(f"[Peer_connection] Marcando {peer} como stale e retirando sua tag de coenxão")
                    self.peer_states.remove_connection(peer)
                    self.peer_states.set_stale()
                    with self.senders_locks_lock:
                        self.senders_locks.pop(peer, None)

                else: #saida certa de quando se fecha a conexão pelo disconnect
                    pass

            #trata dos erros e fecha a conexão de maneira prematura, visto que possivelmente o BYE já não funcionaria
            except Exception as e:
                self.log.warning(f"[Peer_connection] Erro: {e}")
                self.log.debug(f"[Peer_connection] Ending connection with {peer}")
                self.peer_states.remove_connection(peer)
                connected_scokect.close()
                with self.senders_locks_lock:
                    self.senders_locks.pop(peer, None)
                self.peer_states.remove_peer(peer)


    #Responsável por enviar as mensagens para os peers
    #Funções que esperam o Retorno de ACK ou um PONG precisam usar o add_pending_... do state.py antes
    def Sender(self, msg_in: dict, peer_id: str):
        msg = json.dumps(msg_in) + "\n"
        with self.senders_locks_lock:
            lock = self.senders_locks.get(peer_id)
        if lock is None:
            self.log.warning(f"[Peer_connection] Couldn't find the lock from {peer_id}")
            return
        
        with lock:
            try:
                sock = self.peer_states.get_connection(peer_id)
                if sock:
                    sock.send(msg.encode())
                    self.log.debug(f"[Peer_connection] Message sent to: {peer_id}")
                else:
                    self.log.warning(f"[Peer_connection] Scoket to {peer_id} does not exist")
            except Exception as erro:
                if isinstance(erro, (BrokenPipeError, ConnectionResetError)):
                    self.log.warning(f"[Peer_connection] Problem when receiving messages from {peer_id}")
                    self.peer_states.set_stale(peer_id)
                else:
                    self.log.warning(f"[Peer_connection] Got error:{erro} when sending msg to {peer_id}")