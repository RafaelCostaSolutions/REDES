import logging
import time


"""
Resumo resumido:
Usar o objeto PeerTable para:

- armazenar peers conhecidos no state
- atualizar lista de peers
- marcar peers como STALE
- iniciar conexões
- controlar reconexões
"""


class PeerTable:

    # Aqui ele vai receber o state, o servidour de rendezvous e o peer connection
    def __init__(
        self,
        state,
        rend_server,
        peer_connection,
        logger=None
    ):

        self.state = state

        self.rend_server = rend_server

        self.peer_connection = (
            peer_connection
        )

        # Provavelmente nao vai ser necessario, mas cria um logger proprio caso nao seja colocado no parametro
        self.log = (
            logger
            or
            logging.getLogger(
                "PeerTable"
            )
        )

    # Usa o DISCOVER para atualizar a lista de peers
    def refresh_peers(self, namespace=None):

        discovered = self.rend_server.decoberta(namespace)

        current_peers = set()

        for peer in discovered:

            peer_id = f"{peer['name']}@{peer['namespace']}"

            if peer_id == self.state.peer_id:
                continue

            current_peers.add(peer_id)

            info = self.state.get_peer(peer_id)

            if info is None:

                # novo peer → entra como ACTIVE
                self.state.update_peer(
                    peer_id,
                    peer["ip"],
                    peer["port"],
                    peer.get("expires_in"),
                    status="ACTIVE"
                )

            else:

                # 🔥 IMPORTANTE:
                # NÃO sobrescreve status existente
                self.state.update_peer(
                    peer_id,
                    peer["ip"],
                    peer["port"],
                    peer.get("expires_in"),
                    status=info["status"]   # mantém STALE ou ACTIVE
                )

        # remove peers que sumiram do discover
        for peer_id in list(self.state.get_all_peers().keys()):

            if peer_id not in current_peers:

                self.log.debug(
                    "[peer_table] Marcando %s como stale",
                    peer_id
                )

                self.state.set_stale(peer_id)

    # Tenta fazer conexão apenas dos peers ativos
    def connect_new_peers(self):

        peers = self.state.get_all_peers()

        for peer_id, info in peers.items():

            if info["status"] != "ACTIVE":
                continue

            if self.state.get_connection(peer_id):
                continue

            success = self.peer_connection.Connect_Out(
                peer_id,
                info["ip"],
                info["port"]
            )

            if success:
                self.state.update_peer(
                    peer_id,
                    info["ip"],
                    info["port"],
                    info.get("expires_in"),
                    status="ACTIVE"
                )

                self.state.reset_reconnect(peer_id)

                self.log.info(
                    "[PeerTable] Conectado a %s",
                    peer_id
                )

            else:

                self.state.register_failed_attempt(peer_id)


    # Tenta fazer conexão apenas dos peers STALE
    def reconnect_stale_peers(self):

        peers = self.state.get_all_peers()

        for peer_id, info in peers.items():

            if info["status"] != "STALE":
                continue

            reconnect = self.state.get_reconnect_info(peer_id)

            if reconnect is None:
                continue

            attempts = reconnect["attempts"]

            if attempts >= self.state.max_reconnect_attempts:
                continue

            if time.monotonic() < reconnect["next_retry"]:
                continue

            self.log.debug(
                "Tentando reconexão de %s (tentativa %s)",
                peer_id,
                attempts
            )

            success = self.peer_connection.Connect_Out(
                peer_id,
                info["ip"],
                info["port"]
            )

            if success:


                self.state.update_peer(
                    peer_id,
                    info["ip"],
                    info["port"],
                    info.get("expires_in"),
                    status="ACTIVE"
                )

                self.state.reset_reconnect(peer_id)

                self.log.info(
                    "[PeerTable] Reconectado a %s",
                    peer_id
                )

            else:

                self.state.register_failed_attempt(peer_id)