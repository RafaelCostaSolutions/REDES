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

        # Para cada peer descoberto, vai-se atualizar as informacoes de cada peer e marcar como stale os que nao foram descobertos no discover atual

        for peer in discovered:

            peer_id = (
                f"{peer['name']}@"
                f"{peer['namespace']}"
            )

            if (
                peer_id
                ==
                self.state.peer_id
            ):
                continue

            current_peers.add(
                peer_id
            )

            self.state.update_peer(
                peer_id,
                peer["ip"],
                peer["port"],
                peer.get(
                    "expires_in"
                ),
                status="ACTIVE"
            )

        known_peers = (
            self.state.get_all_peers()
        )

        for peer_id in known_peers:

            if (
                peer_id
                not in current_peers
            ):
                print("Marcando", peer_id, "como STALE")

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

            if (
                info["status"]
                !=
                "ACTIVE"
            ):
                continue

            existing = (
                self.state
                .get_connection(
                    peer_id
                )
            )

            if existing:
                continue

            try:


                self.peer_connection.Connect_Out(
                    peer_id,
                    info["ip"],
                    info["port"]
                )

                self.log.info(
                    "[PeerTable] "
                    "Conectado a %s",
                    peer_id
                )

                self.state.reset_reconnect(
                    peer_id
                )

            # Caso a conexão falhar,da o aviso pelo log e registra (para aumentar o tempo do backoff exponencial)
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

                self.log.warning(
                    "[PeerTable] "
                    "Máximo de tentativas "
                    "atingido para %s",
                    peer_id
                )

                continue


            # Verifica se ja deu o tempo de uma nova tentaiva de reconexão.
            if (
                time.monotonic()
                <
                reconnect[
                    "next_retry"
                ]
            ):
                print("É MENOR O TEMPO")
                continue

            try:
                print("TENTATIVA RECONEXAO")
                self.peer_connection.Connect_Out(
                    peer_id,
                    info["ip"],
                    info["port"]
                )
                
                # Se a exceção nao for lançada, a reconexão deu certo. Agora só apagar o histórico de tentativas
                self.state.reset_reconnect(
                    peer_id
                )

                self.log.info(
                    "[PeerTable] "
                    "Reconectado a %s",
                    peer_id
                )

            except Exception as e:
                self.log.warning(
                    "[PeerTable] Falha ao reconectar %s: %s",
                    peer_id,
                    e
                )

                self.state.register_failed_attempt(
                    peer_id
                )