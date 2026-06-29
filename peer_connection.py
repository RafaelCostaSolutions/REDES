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
                self.log.debug(f"[Peer_connection] Connection inbound from {peer_socket}, {addr}")
                #Devido ao protocolo é necessário um aperto de mãos
                peer_socket.settimeout(5.0) #Espera 5 segundos pelo HELLO
                received = peer_socket.recv(1024).decode()
                received = json.loads(received) #Caso receba None ou obtenha algum outro erro o exception irá cuidar


                #Verificação se a mensagem recebida é um Hello com uma id
                if received.get('type') != "HELLO" or 'peer_id' not in received:
                    self.log.debug(f"[Peer_connection] Wrong Handshake protocoll")
                    peer_socket.close()
                    continue
                
                #Verificação se o peer já não está conectado, se ja está não se deve conectar novamente
                peer = received.get('peer_id')
                if peer in self.peer_states.get_all_connections().keys():
                    self.log.debug(f"[Peer_connection] Peer is already connected")
                    peer_socket.close()
                    continue
                    
                self.log.debug(f"[Peer_connection] Connection stabilished with {peer}")

                #coletando informações e guardando elas
                self.peer_states.update_peer(peer, addr[0], addr[1]) #updates the state table
                self.peer_states.add_connection(peer, peer_socket,"INBOUND") #passa o socket e direção para a state table
                with self.senders_locks_lock:
                    self.senders_locks[peer] = threading.Lock()

                #respondendo
                resposta = {"type":"HELLO_OK","peer_id":self.my_peer_id,"version":"1.0","features":self.features,"ttl":self.peer_ttl}
                self.Sender(resposta, peer)

                #iniciando a porta de escuta para o que esse peer enviar
                tr = threading.Thread(target=self._receiver_handler, args=[peer]) #Começa o processo de ouvir o que o peer enviar
                tr.start()
                with self.thread_ativas_lock:
                    self.threads_ativas[peer] = tr

            except OSError:
                if not self.listening.is_set():
                    # socket fechado intencionalmente pelo Full_disconnect
                    self.log.debug(f"[Peer_connection] Listener closed intentionally to disconnect")

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
            self.log.debug(f"[Peer_connection] Connection outbound - Sent to: {peer_id}")
            tipo = resposta.get('type')

            if tipo == "HELLO_OK": #conexão aceita, adiciona as informações nos states
                sock.settimeout(None)
                with self.senders_locks_lock:
                    self.senders_locks[peer_id] = threading.Lock()
                    lock_created = True
                self.peer_states.add_connection(peer_id, sock, "OUTBOUND")
                connection_added = True
                tr = threading.Thread(target=self._receiver_handler, args=[peer_id]) #Começa o processo de ouvir, igual ao do First_contact
                tr.start()
                with self.thread_ativas_lock:
                    self.threads_ativas[peer_id] = tr
                return True

            else:
                self.log.debug(f"[Peer_connection] Failed to connect with {peer_id}")
                with self.senders_locks_lock:
                    self.senders_locks.pop(peer_id, None)
                self.peer_states.remove_connection(peer_id)
                self.peer_states.set_stale(peer_id)

        except Exception as error:
            self.log.debug(f"[Peer_connection] Failed to connect to {peer_id} reson: {error}")
            if lock_created:
                with self.senders_locks_lock:
                    self.senders_locks.pop(peer_id, None)
            if connection_added:
                self.peer_states.remove_connection(peer_id)
            self.peer_states.set_stale(peer_id)
            if sock is not None:
                try:
                    sock.close()
                    raise
                except Exception:
                    raise

    #Chamada quando um peer pede para se desconectar    
    def _disconnect_inbound(self, msg, peer): 
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
        
        with self.thread_ativas_lock: #adquire cada thread, para não causar erros com lock no join
            thread_ativas_list = list(self.threads_ativas.values())
            self.threads_ativas.clear()

        for j in thread_ativas_list: #garante o encerramento de cada thread
                j.join(timeout=2.0)

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
                if (retorno.get('type') == 'BYE_OK') and (retorno.get('msg_id') == specific_uuid):
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
        connected_scokect.settimeout(2)
        max_msg = 32768

        #buffer usado para receber as mensagens,
        #necessário pois conexão TCP não garabet tudo chegar junto (dmorou para lembrar disso)
        buffer = b""

        while (self.listening.is_set()):
            try:
                try:
                    if self.listening.is_set():
                        part = connected_scokect.recv(32768)

                    else:
                        break
                    
                    if not part:
                        self.log.debug(f"[Peer_connection] Received malformed msg from {peer}")
                        break

                    buffer = buffer + part

                    #quando se tem pelo menso um \n no buffer, quer dizer que pelo menso uma mensagem chegou
                    while b"\n" in buffer: 
                        recebido, buffer = buffer.split(b"\n", 1) #passa a mensagem que ja chegou para o recebido, enquanto o resto continua no buffer

                        #checa para ver se esta dentro do limite de tamanho
                        if len(recebido) > max_msg:
                            self.log.debug(f"[MSG] {peer}: Exceeded the limit of characters")
                            continue

                        try:
                            recebido = json.loads(recebido.decode())
                            tempo = datetime.now(timezone.utc).isoformat()
                            tipo = recebido.get('type')
                        except json.JSONDecodeError:
                            self.log.debug(f"[Peer_connection] Malformed message from {peer}")
                            continue

                        self.log.debug(f"[Peer_connection] Message received from {peer}: {recebido}")

                        if tipo == "PING":
                            if self.peer_states.get_connection(peer) != None: #se a conexão ja foi fechada não se deve mandar o ping de volta
                                msg = {'type':"PONG", 'msg_id':recebido.get('msg_id'), "timestamp":tempo, "ttl":self.peer_ttl}
                                self.Sender(msg, peer)
                            else:
                                pass

                        elif tipo == "SEND":
                            self.log.info(f"[MSG] {peer}: {recebido.get('payload')}")
                            if recebido.get('require_ack'):
                                msg = {'type':"ACK", 'msg_id':recebido.get('msg_id'), "timestamp":tempo, "ttl":self.peer_ttl}
                                self.Sender(msg, peer)

                        elif tipo == "PUB":
                            self.log.info(f"[MSG] {peer}: {recebido.get('payload')}")
                            if recebido.get('require_ack'):
                                msg = {'type':"ACK", 'msg_id':recebido.get('msg_id'), "timestamp":tempo, "ttl":self.peer_ttl}
                                self.Sender(msg, peer)

                        elif tipo == "BYE":
                            if self.listening.is_set():
                                self.log.debug(f"[Peer_connection] Asked to close connection from {peer}, reason: {recebido.get('reason')}")
                                self._disconnect_inbound(recebido, peer)
                            return

                        elif tipo == "PONG":
                            uuid = recebido.get('msg_id')
                            pong_received = time.monotonic()
                            ping_sent = self.peer_states.get_pending_ping_time(uuid)

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

                    #erros de timeout são esperados, visto que o recv é fechado a cada 2 segundos para não travar quando fechar
                    if "timed out" in str(error).lower():
                        continue
                    
                    #erros não esperados de socket devem então ser tratados 
                    if (self.listening.is_set()):
                        self.log.warning(f"[Peer_connection] Erro: {error}")
                        self.log.debug(f"[Peer_connection] Marcando {peer} como stale e retirando sua tag de conexão")

                        #quando tal erro ocorre deve-se marcar o peer como stale para tentar a reconexão
                        self.peer_states.remove_connection(peer)
                        self.peer_states.set_stale(peer)
                        with self.senders_locks_lock:
                            self.senders_locks.pop(peer, None)
                        break

                    #nesse caso o erro ocorre devido ao disconnect, e não se deve fechar as conexão ainda
                    else:
                        break
            
            #erros inesperados, nesse caso foi escolhido que o peer seria completamente apagado, paar se tentar novamente em um futuro discover
            except Exception as e:
                self.log.warning(f"[Peer_connection] Erro: {e}")
                self.log.debug(f"[Peer_connection] Ending connection with {peer}")
                self.peer_states.remove_connection(peer)
                with self.senders_locks_lock:
                    self.senders_locks.pop(peer, None)
                self.peer_states.remove_peer(peer)
                break



    #Responsável por enviar as mensagens para os peers
    #Funções que esperam o Retorno de ACK ou um PONG precisam usar o add_pending_... do state.py antes
    def Sender(self, msg_in: dict, peer_id: str):
        msg = json.dumps(msg_in) + "\n"

        #{msg.encode()!r} usado para ter exatamente o que esta sendo mandado
        self.log.debug(f"[Peer_connection] Sending message: {msg.encode()!r} to {peer_id}")


        with self.senders_locks_lock:
            lock = self.senders_locks.get(peer_id)

        if lock is None:
            self.log.debug(f"[Peer_connection] Couldn't find the lock from {peer_id}")
            return False
        
        with lock:
            try:
                sock = self.peer_states.get_connection(peer_id)

                if sock:
                    sock.send(msg.encode())
                    return True

                else:
                    self.log.warning(f"[Peer_connection] Scoket to {peer_id} does not exist")
                    return False

            except Exception as erro:
                
                #erros devido a conexão socket, logo é feito um tratamento para tal
                if isinstance(erro, (BrokenPipeError, ConnectionResetError)):
                    self.log.warning(f"[Peer_connection] Problem when sending messages to {peer_id}")

                    self.peer_states.remove_connection(peer_id)
                    self.peer_states.set_stale(peer_id)

                    with self.senders_locks_lock:
                        self.senders_locks.pop(peer_id, None)

                    if sock:
                        try:
                            sock.close()
                        except Exception:
                            pass
                    
                    return False
                
                #erros inesperados
                else:
                    self.log.warning(f"[Peer_connection] Got error:{erro} when sending msg to {peer_id}")