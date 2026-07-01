import json
import time
import uuid
import logging
import threading


"""
Resumo resumido:

- enviar SEND;
- enviar PUB;
- enviar ACK;
- controlar timeout de ACK (5s);
"""


class MessageRouter:

    def __init__(
        self,
        state,
        peer_connection,
        logger=None
    ):

        self.state = state

        self.peer_connection = peer_connection

        self.log = (
            logger
            or logging.getLogger("Router")
        )

    # Auto Explicativo
    def send_message(
        self,
        dst,
        payload,
        require_ack=True
    ):

        # Gera id único da mensagem
        msg_id = str(uuid.uuid4())

        msg = {

            "type": "SEND",

            "msg_id": msg_id,

            "src": self.state.peer_id,

            "dst": dst,

            "payload": payload,

            "require_ack": require_ack,

            "ttl": 1
        }

        # Tenta enviar via PeerConnection
        ok = self.peer_connection.Sender(msg, dst)

        if not ok:

            self.log.debug(
                "Falha ao enviar para %s",
                dst
            )

            return False

        self.log.debug(
            "SEND %s: %s",
            dst,
            payload
        )

        # Se precisar de ACK, adiciona pendente e inicia watcher
        if require_ack:

            self.state.add_pending_ack(
                msg_id,
                dst
            )

            threading.Thread(
                target=self._wait_ack,
                args=(msg_id,),
                daemon=True
            ).start()

        return True

    # Espera ACK por 5 segundos e verifica timeout
    def _wait_ack(
        self,
        msg_id
    ):

        time.sleep(5)

        pending = self.state.get_pending_acks()

        if msg_id in pending:

            info = pending[msg_id]

            self.log.warning(
                "ACK timeout para %s",
                info["peer_id"]
            )

            self.state.remove_pending_ack(
                msg_id
            )

    # Auto Explicativo!
    def publish(
        self,
        dst,
        payload
    ):

        msg_id = str(
            uuid.uuid4()
        )

        peers = (
            self.state.get_all_peers()
        )

        # Se eu também faço parte do destino, exibe a mensagem localmente
        if (
            dst == "*"
            or
            self.state.namespace == dst[1:]
        ):

            self.process_pub(
                {
                    "src": self.state.peer_id,
                    "payload": payload
                }
            )

        for peer_id in peers:

            if (
                peer_id
                ==
                self.state.peer_id
            ):
                continue

            # Se não for broadcast, envia apenas para o namespace informado
            if dst.startswith("#"):

                namespace = (
                    peer_id.split(
                        "@",
                        1
                    )[1]
                )

                if (
                    namespace
                    !=
                    dst[1:]
                ):
                    continue

            msg = {

                "type": "PUB",

                "msg_id": msg_id,

                "src": self.state.peer_id,

                "dst": peer_id,

                "payload": payload,

                "require_ack": False,

                "ttl": 1
            }

            success = (
                self.peer_connection.Sender(
                    msg,
                    peer_id
                )
            )

            if not success:

                self.log.debug(
                    "Falha ao enviar PUB para %s",
                    peer_id
                )

        self.log.debug(
            "PUB (%s): %s",
            dst,
            payload
        )

    # Auto Explicativo!
    def process_pub(
        self,
        msg
    ):

        src = msg.get("src", "unknown")

        payload = msg.get("payload", "")

        print(
            f"\n[PUB] {src}: {payload}"
        )