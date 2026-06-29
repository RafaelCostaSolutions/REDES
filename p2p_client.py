import threading
import logging
import time

from state import State
from peer_connection import PeerConnection
from peer_table import PeerTable
from keep_alive import Keep_Alive
from message_router import MessageRouter
from rend_connect import RendServer


"""
Resumo resumido:

Esse arquivo, simplificando, apenas inicializa os serviços, não faz nada por si mesmo.
Logo, não vejo necessidade de colocar muitos comentários no código
"""


class P2PClient:

    def __init__(self, config: dict):

        self.log = logging.getLogger("P2PClient")

        self.state = State(config)

        self.rend = RendServer(
            host_id=config["rendezvous_host"],
            host_port=config["rendezvous_port"],
            logs=self.log
        )

        self.peer_conn = PeerConnection(
            my_ip="0.0.0.0",
            my_port=config["listen_port"],
            features=config['features'],
            states=self.state,
            logs=self.log
        )

        self.router = MessageRouter(
            state=self.state,
            peer_connection=self.peer_conn,
            logger=self.log
        )

        self.peer_table = PeerTable(
            state=self.state,
            rend_server=self.rend,
            peer_connection=self.peer_conn,
            logger=self.log
        )

        self.keep_alive = Keep_Alive(
            logs=self.log
        )

        self.config = config

        self.running = threading.Event()
        self.running.set()

        self.worker_thread = None

    def start(self):

        self.log.info("P2PClient Iniciando...")


        # Começa a esperar conexões de peers
        self.peer_conn.Start(
            self.state.name,
            self.state.namespace
        )


        # Faz possível peers te "enxergarem"
        self.rend.registrar(
            self.state.namespace,
            self.state.name,
            self.state.listen_port,
            self.config.get("rdv_ttl")
        )


        # Inicia os pings de tempo em tempo
        self.keep_alive.Start(
            self.state,
            self.peer_conn,
            self.config.get("keepalive_interval", 30),
            ttl=1
        )


        # Essa thread é usada para conexões, discovery e reconexão
        self.worker_thread = threading.Thread(
            target=self._network_loop,
            daemon=True
        )
        self.worker_thread.start()

        self.log.info("P2PClient Iniciou")

    def _network_loop(self):

        interval = self.config.get('discover_interval')

        while self.running.is_set():

            try:
                self.peer_table.refresh_peers()
                self.peer_table.connect_new_peers()
                self.peer_table.reconnect_stale_peers()
            except Exception as e:
                self.log.warning("Erro no loop de rede: %s", e)

            time.sleep(interval)

    def send_message(self, peer_id, text):
        return self.router.send_message(peer_id, text)

    def publish(self, dst, text):
        return self.router.publish(dst, text)

    def get_peers(self, escopo):


        # Tratamento da string escopo
        if escopo is None:
            self.log.warning("Escopo inválido!")
            return

        if escopo == "*":
            escopo = None

        elif escopo.startswith("#"):
            escopo = escopo[1:]  # remove o #

        else:
            self.log.warning("Escopo inválido!")
            return
        
        peers = self.rend.decoberta(escopo)
        dict_peers = {}
        my_id = self.state.get_peer_id()

        for i in peers:
            if i.get('name') + '@' + i.get('namespace') != my_id:
                dict_peers.setdefault(i.get('namespace'),[]).append(i)

        for j in dict_peers:
            self.log.info(f"#{j} :")
            for k in dict_peers[j]:
                self.log.info(f"- {k.get('name')} @ {k.get('ip')}:{k.get('port')}")        

    def get_connections(self):
        return self.state.get_all_connections()
    
    def show_all_connections(self):
        with self.state.connections_lock:
            if not self.state.connections:
                return "Nenhuma conexão ativa."

            linhas = []

            for peer_id, info in self.state.connections.items():
                linhas.append(
                    f"{peer_id} | {info['direction']}"
                )

            return "\n".join(linhas)

    def get_rtt(self, peer_id):
        return self.state.get_rtt(peer_id)
    
    def show_all_rtt(self):

        all_rtt = self.state.get_all_rtt()

        if not all_rtt:

            print("Nenhum RTT disponível.")

            return

        for peer_id, stats in all_rtt.items():

            self.log.info(
                f"{peer_id}: "
                f"médio={stats['average']:.2f} ms | "
            )

    def reconnect(self):
        return self.peer_table.reconnect_stale_peers()

    def shutdown(self):

        self.log.info("Desligando P2PClient...")

        self.running.clear()

        try:
            self.keep_alive.Stop()
        except:
            pass

        try:
            self.peer_conn.Full_disconnect()
        except:
            pass

        try:
            self.rend.fechar_conexão(
                self.state.namespace,
                self.state.name,
                self.state.listen_port
            )
        except:
            pass

        self.log.info("P2PClient parou")