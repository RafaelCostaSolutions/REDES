import threading
import time



"""
Resumo resumido:
Usar o objeto state para guardar TODAS as informacoes, como:

- Peer local
- Lista de peers e seus estados
- Conexoes ativas entre os peers
- Pings e Acks
- Informacoes do RTT
- etc

Toda vez que for settar ou pegar qualquer informacao usar o 

with state_*_lock:

para evitar problemas de concorrencia

"""


# Objeto para guardar estado e acessar informaçoes
class State:

    def __init__(self, config):

        self.peer_id = config.get(
            "peer_id",
            "user@namespace"
        )
        
        self.name = ""
        self.namespace = ""
        if "@" in self.peer_id:

            self.name, self.namespace = (
                self.peer_id.split(
                    "@",
                    1
                )
            )

        self.listen_port = config.get(
            "listen_port",
            9000
        )

        self.running = threading.Event()
        self.running.set()

        self.peers = {}
        self.peers_lock = threading.Lock()

        self.connections = {}
        self.connections_lock = threading.Lock()

        self.pending_acks = {}
        self.acks_lock = threading.Lock()

        self.pending_pings = {}
        self.pings_lock = threading.Lock()

        self.rtt = {}
        self.rtt_lock = threading.Lock()

        self.reconnect_info = {}
        self.reconnect_lock = threading.Lock()

        self.max_reconnect_attempts = (
            config.get(
                "max_reconnect_attempts",
                5
            )
        )

    # Mudar informacoes do peer na "lista" dos peers
    def update_peer(
        self,
        peer_id,
        ip,
        port,
        expires_in=None,
        status="ACTIVE"
    ):

        with self.peers_lock:

            self.peers[peer_id] = {
                "ip": ip,
                "port": port,
                "expires_in": expires_in,
                "status": status
            }

    # Auto explicativo!
    def get_peer(
        self,
        peer_id
    ):

        with self.peers_lock:
            return self.peers.get(peer_id)

    # Auto explicativo!
    def get_all_peers(
        self
    ):

        with self.peers_lock:
            return dict(self.peers)
        

    # Remove peer da "lista" de peers
    def remove_peer(
        self,
        peer_id
    ):

        with self.peers_lock:
            self.peers.pop(peer_id, None)


    # Peer nao responde? marca ele como STALE
    def set_stale(
        self,
        peer_id
    ):

        with self.peers_lock:

            if peer_id in self.peers:
                self.peers[peer_id]["status"] = "STALE"


    # Guarda o socket
    def add_connection(
        self,
        peer_id,
        conn
    ):

        with self.connections_lock:
            self.connections[peer_id] = conn


    # Recupera o socket
    def get_connection(
        self,
        peer_id
    ):

        with self.connections_lock:
            return self.connections.get(peer_id)
        

    # Remove o socket
    def remove_connection(
        self,
        peer_id
    ):

        with self.connections_lock:
            self.connections.pop(peer_id, None)


    # Auto explicativo!
    def add_pending_ack(
        self,
        msg_id,
        peer_id
    ):

        with self.acks_lock:

            self.pending_acks[msg_id] = {
                "peer_id": peer_id,
                "timestamp": time.time()
            }


    # Auto explicativo!
    def remove_pending_ack(
        self,
        msg_id
    ):

        with self.acks_lock:
            self.pending_acks.pop(msg_id, None)

    # Auto explicativo!
    def add_pending_ping(
        self,
        msg_id
    ):

        with self.pings_lock:
            self.pending_pings[msg_id] = time.time()


    # Auto explicativo!
    def remove_pending_ping(
        self,
        msg_id
    ):

        with self.pings_lock:
            self.pending_pings.pop(msg_id, None)

    # Auto explicativo!
    def set_rtt(
        self,
        peer_id,
        value
    ):

        with self.rtt_lock:

            if peer_id not in self.rtt:

                self.rtt[peer_id] = {
                    "count": 0,
                    "total": 0.0,
                    "average": 0.0,
                    "min": value,
                    "max": value
                }

            stats = self.rtt[peer_id]

            stats["count"] += 1
            stats["total"] += value

            stats["average"] = (
                stats["total"] /
                stats["count"]
            )

            stats["min"] = min(
                stats["min"],
                value
            )

            stats["max"] = max(
                stats["max"],
                value
            )

    # Auto explicativo!
    def get_rtt(
        self,
        peer_id
    ):

        with self.rtt_lock:
            return self.rtt.get(peer_id)
        


    # Resumo da função: Conexão falhou? É feito o backoff exponencial
    def register_failed_attempt(
        self,
        peer_id
    ):

        with self.reconnect_lock:

            info = self.reconnect_info.get(
                peer_id,
                {
                    "attempts": 0
                }
            )

            info["attempts"] += 1

            delay = (
                2 **
                (
                    info["attempts"] - 1
                )
            )

            info["next_retry"] = (
                time.time() +
                delay
            )

            self.reconnect_info[peer_id] = info


    # Faz consulta do numero de tentativas e o momento que uma nova tentiva é permitida
    def get_reconnect_info(
        self,
        peer_id
    ):

        with self.reconnect_lock:

            info = self.reconnect_info.get(
                peer_id
            )

            if info is None:
                return None

            return dict(info)
        

    # Resetar conexão = Apagar histórico
    def reset_reconnect(
        self,
        peer_id
    ):

        with self.reconnect_lock:
            self.reconnect_info.pop(
                peer_id,
                None
            )