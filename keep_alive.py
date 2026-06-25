"""
Keep_Alive - Responsável por mandar pings para os peers periodicamente
     Funções públicas:
        Start(State: do state.py, Peer_connect: o server do peer_connection, ping_intervall)
            → Começa o processo de mandar os Pings periodicamente
        Stop()
            → Para o processo assim que chamado
"""

import logging
import threading
from uuid import uuid4
from datetime import datetime, timezone

from peer_connection import PeerConnection
from state import State


class Keep_Alive():
    def __init__(self, logs: logging.Logger = None):
        self.log = logs
        self.ttl = None
        self.intervall = None
        self.peer_states: State = None
        self.peer_conn: PeerConnection = None
        self.running = threading.Event()
        self.wait_time = threading.Event()
        self.thread_run = None

    #Auto explicativo
    def Start(self, State: State, Peer_conect: PeerConnection, ping_intervall: int, ttl: int = None):
        self.peer_states = State
        self.peer_conn = Peer_conect
        self.intervall = ping_intervall
        self.ttl = ttl

        self.log.info(f"[Keep_Alive] Starting process")

        self.running.set()
        self.wait_time.clear()
        tr = threading.Thread(target=self._run)
        self.thread_run = tr
        tr.start()

    #Auto explicativo
    def Stop(self):
        self.log.info(f"[Keep_Alive] Terminating process")
        self.running.clear()
        self.wait_time.set()
        self.thread_run.join()
        self.log.debug(f"[Keep_Alive] Process ended")
        
    
    def _run(self):
        States = self.peer_states
        peer_serv = self.peer_conn
        intervall = self.intervall
        ttl = self.ttl

        while self.running.is_set():
            present_peers = States.get_all_peers()

            # Manda ping para todos os peers cada um com um uuid específico
            for i in present_peers:
                info = States.get_peer_info(i) #informaç~eos de cada peer

                if info.get('status') == "ACTIVE": #só se deve mandar ping para os peers ativos

                    if self.running.is_set(): #feito que, caso se use o stop() enquanto há uma varredura de pings, ele não tenha de esperar todo o tempo
                        unique_uuid = str(uuid4())
                        States.add_pending_ping(unique_uuid)

                        msg = {"type": "PING","msg_id": unique_uuid,"timestamp": datetime.now(timezone.utc).isoformat(),"ttl": ttl}

                        try:
                            self.log.debug(f"[Keep_Alive] Sending PING to {i}")
                            peer_serv.Sender(msg, i)

                        except Exception as error:
                            self.log.warning(f"[Keep_Alive] Got {error} when sending ping to {i}")

            if self.wait_time.wait(timeout=intervall): #mesmo motivo de garantir que o stop não fique preso por 5 segundos quando o processo parar
                break
