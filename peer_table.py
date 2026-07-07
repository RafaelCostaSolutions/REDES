import logging
import time
import threading


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
        self.connecting_peers = set()
        self.connecting_lock = threading.Lock()

    # Usa o DISCOVER para atualizar a lista de peers
    def refresh_peers(
        self,
        namespace=None
    ):

        discovered = (
            self.rend_server.decoberta(
                namespace
            )
        )

        current_peers = set()


        for peer in discovered:

            peer_id = (
                f"{peer['name']}@"
                f"{peer['namespace']}"
            )


            if peer_id == self.state.peer_id:
                continue


            current_peers.add(peer_id)


            if self.state.get_peer(peer_id):

                self.state.update_peer(
                    peer_id,
                    peer["ip"],
                    peer["port"],
                    peer.get("expires_in")
                )


            else:

                # Não coloca ACTIVE aqui.
                # Só descobriu, ainda não conectou.
                self.state.update_peer(
                    peer_id,
                    peer["ip"],
                    peer["port"],
                    peer.get("expires_in"),
                    status="STALE"
                )



        known_peers = (
            self.state.get_all_peers()
        )


        for peer_id in known_peers:

            if peer_id not in current_peers:

                self.log.debug(
                    f"[peer_table] Marcando {peer_id} como stale"
                )

                self.state.set_stale(
                    peer_id
                )

    # Tenta fazer conexão apenas dos peers ativos
    def connect_new_peers(
        self
    ):

        peers = (
            self.state.get_all_peers()
        )


        for (
            peer_id,
            info
        ) in peers.items():


            if info["status"] != "ACTIVE":
                continue


            existing = (
                self.state.get_connection(
                    peer_id
                )
            )


            if existing:
                continue



            # Impede duas threads conectando no mesmo peer
            with self.connecting_lock:

                if peer_id in self.connecting_peers:
                    continue

                self.connecting_peers.add(
                    peer_id
                )


            try:

                con = self.peer_connection.Connect_Out(
                    peer_id,
                    info["ip"],
                    info["port"]
                )


                if con:

                    # Agora sim ele está conectado
                    self.state.set_active(
                        peer_id
                    )


                    self.state.reset_reconnect(
                        peer_id
                    )


                    self.log.info(
                        "[PeerTable] Conectado a %s",
                        peer_id
                    )


                else:

                    self.state.register_failed_attempt(
                        peer_id
                    )



            except Exception as error:


                self.log.warning(
                    "[PeerTable] "
                    "Falha ao conectar "
                    "%s (%s)",
                    peer_id,
                    error
                )


                self.state.register_failed_attempt(
                    peer_id
                )


            finally:

                # Libera para futuras tentativas
                with self.connecting_lock:

                    self.connecting_peers.discard(
                        peer_id
                    )


    # Tenta fazer conexão apenas dos peers STALE
    def reconnect_stale_peers(
        self
    ):

        peers = (
            self.state.get_all_peers()
        )

        for (
            peer_id,
            info
        ) in peers.items():           

            # Aqui, é verificado se houve pelo menos uma tentiva de reconexão. Se não houver, pula o peer. Mas se houver tenta fazer a reconexão.
            reconnect = (
                self.state
                .get_reconnect_info(
                    peer_id
                )
            )

            if reconnect is None:
                continue

            attempts = (
                reconnect[
                    "attempts"
                ]
            )

            # Se o numero de tentativas de reconexão passar do máximo, pula o peer.
            if (
                attempts
                >=
                self.state
                .max_reconnect_attempts
            ):
                continue


            # Verifica se ja deu o tempo de uma nova tentaiva de reconexão.
            if (
                time.monotonic()
                <
                reconnect[
                    "next_retry"
                ]
            ):
                continue

            try:
                self.log.debug(f"Tentando reconexão de {peer_id}, tentativa: {attempts}")
                with self.connecting_lock:

                    if peer_id in self.connecting_peers:
                        continue

                    self.connecting_peers.add(peer_id)
                try:
                    success = self.peer_connection.Connect_Out(
                        peer_id,
                        info["ip"],
                        info["port"]
                    )

                    if success:
                        self.state.set_active(peer_id)
                        self.state.reset_reconnect(peer_id)
                    else:
                        self.state.register_failed_attempt(peer_id)
                finally:
                    with self.connecting_lock:
                        self.connecting_peers.discard(peer_id)


                self.log.info(
                    "[PeerTable] "
                    "Reconectado a %s",
                    peer_id
                )

            except Exception:
                if (attempts == self.state.max_reconnect_attempts):
                    self.log.warning(
                    "[PeerTable] "
                    "Máximo de tentativas "
                    "atingido para %s",
                    peer_id
                )
                    
                self.state.register_failed_attempt(
                    peer_id
                )

    def is_connecting(self, peer_id):

        with self.connecting_lock:
            return peer_id in self.connecting_peers