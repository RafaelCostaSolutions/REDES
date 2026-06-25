"""
Rend_connect - Responsável por fazer a conexão inicial com o servidor Rendezvous
Tasks intermitentes: renovar a conexão até a finalisação do programa

Classe: RendServer
Funções públicas:
    registrar(namespace, name, listen_port, ttl[opcional])
        → Faz o registro no servidor, e gerencia esse para ser refreshed quando ter se passado o TTL

    decoberta(namespace[opcional])
        → Retorna uma listas de peers no namespace passado (caso esse não seja passado retorna todos os peers)
        → A sua chamada recorrente deverá ser feita em outro programa

    fechar_conexão(namespace, name, listen_port)
        →Encerra a conexão com o server

    O _sender é o responsável por enviar as mensagens ao servidor, para poder se usar diversas threads esse usa um .lock, como "em_uso"
    
"""

import json
import socket
import logging
import threading


class Error_Falha_Server(Exception):
    pass

class RendServer:
    def __init__(self, 
                host_id: str,
                host_port: int,
                logs: logging.Logger = None):
        
        self.host = host_id
        self.port = host_port
        self.log = logs
        self.em_uso = threading.Lock()
        self.encerrar = threading.Event()
        self.auto_reregister = None

    #Faz o registro inicial        
    def registrar(self, namespace: str, name: str, listen_port: str, ttl: int = 7200):
        self.log.info(f"[RDV] Se registrando como {name}@{namespace} por {ttl}s")

        msg = {"type": "REGISTER", "namespace": namespace, "name": name, "port": listen_port, "ttl": ttl}

        resposta = self._sender(msg)

        if resposta['status'] != "OK":
            raise Error_Falha_Server("[RDV] Type of message received is missmatched")
        
        
        self.log.debug(f"[RDV] Sucesso em se registrar como {name}@{namespace} por {ttl}s")
        self.log.debug(f"[RDV] Iniciando processo de auto reconnect")
        
        self.auto_reregister = threading.Thread(target=self._reconect, args=[namespace, name, listen_port, ttl], daemon=True) #daemon garante que caso o programa se encerre a thread também feche
        self.auto_reregister.start()

        return resposta

    # Realisa a conexão peródica com o ttl setado
    def _reconect(self, namespace: str, name: str, listen_port: str, ttl: int = 7200):
        while not (self.encerrar.is_set()):
            self.encerrar.wait(ttl)
            if not(self.encerrar.is_set()): #para garantir que não sera mandado após o fechamento da conexão
                msg = { "type": "REGISTER", "namespace": namespace, "name": name, "port": listen_port, "ttl": ttl}
                reconected = self._sender(msg)

            if reconected['status'] != "OK":
                raise Error_Falha_Server("[RDV] Type of message received is missmatched")
            
            self.log.debug(f"[RDV] Sucesso em refrescara conexão com o servidor")

    #Função auxiliar para retornar a lista de peers 
    def decoberta(self, namespace: str = None):
        self.log.debug("[RDV] Requisitando lista de peers")

        msg = {"type": "DISCOVER", "namespace": namespace}

        list_peers = self._sender(msg)

        if list_peers['status'] != "OK":
            raise Error_Falha_Server("[RDV] Type of message received is missmatched")

        list_peers = list_peers.get('peers')
        if list_peers == []:
            self.log.info("[RDV] No peers in this namespace, or it does not exist")
            return []
        final_list_peers = []

        if isinstance(list_peers, list) == False:
            raise Error_Falha_Server("[RDV] Not received a list for the peers")
        
        self.log.debug("[RDV] Lista de peers Obtida")

        for i in list_peers:
            if (isinstance(i, dict)):
                    final_list_peers.append(i)

        return final_list_peers

    #Processo de fechar a conexão, causa thread de refresh a se encerrar
    def fechar_conexão(self, namespace: str, name: str, listen_port: int):
        self.log.info("[RDV] Terminando conexão com o server")

        self.encerrar.set()

        msg = {"type": "UNREGISTER", "namespace": namespace, "name": name, "port": listen_port}

        success_close = self._sender(msg)

        if success_close['status'] != "OK":
            raise Error_Falha_Server("[RDV] Type of message received is missmatched")
        
        self.log.info(f"[RDV] Temino da conxão {name}@{namespace} confirmado")
        return

    #Função utilisada para enviar as mensagens ao servidor
    def _sender(self, msg_in):
        self.log.debug(f"[RDV] Preparando para enviar: {msg_in}")
        msg = json.dumps(msg_in) + "\n"

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host , self.port))

            try:
                with self.em_uso:
                    sock.send(msg.encode())

            except Exception as error:
                self.log.info(f"[RDV] Erro ao mandar mensagem: {error}")

            retorno = sock.recv(32768)
            self.log.debug(f"[RDV] Receido: {retorno}")

            try:
                if retorno is None:
                    raise Error_Falha_Server("[RDV] Got None instead of a message")
                        
                retorno = json.loads(retorno)
                return retorno

            except:
                raise Error_Falha_Server("[RDV] Message recived is not a json")
            
        except:
            self.log.warning(f"[RDV] Could not connect to the server, closing socket")
            try:
                sock.close()
            except:
                Error_Falha_Server("[RDV] Falha crítica, necessário reiniciar o programa")