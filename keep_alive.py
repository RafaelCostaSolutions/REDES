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
import time
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

        while self.running.is_set():

            pending = States.get_pending_ping_peers()

            #
            # Verifica timeouts
            #
            for msg_id, info in pending.items():

                peer = info[0]
                sent = info[1]

                if time.monotonic() - sent < self.intervall:
                    continue

                self.log.debug(
                    "[Keep_Alive] %s did not answer ping",
                    peer
                )

                States.remove_pending_ping(msg_id)
                States.set_stale(peer)

            #
            # Envia novos pings
            #
            present_peers = States.get_all_peers()

            for peer_id, info in present_peers.items():

                if info["status"] != "ACTIVE":
                    continue

                if States.get_connection(peer_id) is None:
                    continue

                msg_id = str(uuid4())

                msg = {
                    "type": "PING",
                    "msg_id": msg_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ttl": self.ttl
                }

                try:

                    States.add_pending_ping(msg_id, peer_id)

                    self.log.debug(
                        "[Keep_Alive] Sending PING to %s",
                        peer_id
                    )

                    peer_serv.Sender(msg, peer_id)

                except Exception as e:

                    States.remove_pending_ping(msg_id)

                    self.log.warning(
                        "[Keep_Alive] Error sending ping to %s: %s",
                        peer_id,
                        e
                    )

            if self.wait_time.wait(timeout=self.intervall):
                break
