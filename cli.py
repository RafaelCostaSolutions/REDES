"""
cli.py — Interface de Linha de Comando do cliente P2P
Responsável por ler comandos do usuário e delegar às camadas corretas.
"""

import logging
import shlex

logger = logging.getLogger("P2PClient")


HELP_TEXT = """
╔══════════════════════════════════════════════════════════════╗
║                  Comandos disponíveis                        ║
╠══════════════════════════════════════════════════════════════╣
║  /peers [* | #namespace]   Listar peers descobertos          ║
║  /msg <peer_id> <texto>    Enviar mensagem direta            ║
║  /pub * <texto>            Broadcast global                  ║
║  /pub #<namespace> <texto> Mensagem para todo o namespace    ║
║  /conn                     Mostrar conexões ativas           ║
║  /rtt                      Exibir RTT médio por peer         ║
║  /reconnect                Forçar reconciliação de peers     ║
║  /log <NIVEL>              Ajustar nível de log              ║
║  /help                     Exibir esta ajuda                 ║
║  /quit                     Encerrar aplicação                ║
╚══════════════════════════════════════════════════════════════╝
"""

# Níveis de log válidos
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class CLI:
    """
    Lê comandos do stdin e os despacha para o cliente P2P.

    O cliente P2P (`p2p_client`) deve implementar:
        - send_message(dst_peer_id, text)
        - publish_message(dst, text)         # dst = '*' ou '#namespace'
        - list_peers(scope)                  # scope = '*' | '#ns' | None
        - show_connections()
        - show_rtt()
        - force_reconnect()
        - shutdown()
    """

    def __init__(self, p2p_client):
        self.client = p2p_client
        self.running = False

    # ------------------------------------------------------------------
    # Loop principal
    # ------------------------------------------------------------------

    def run(self):
        """Inicia o loop de leitura de comandos (bloqueante)."""
        self.running = True
        print(HELP_TEXT)
        logger.info("CLI iniciada. Digite /help para ver os comandos.")

        while self.running:
            try:
                raw = input(">> ").strip()
            except (EOFError, KeyboardInterrupt):
                # Ctrl+D ou Ctrl+C encerram graciosamente
                self._cmd_quit([])
                break

            if not raw:
                continue

            self._dispatch(raw)

    def stop(self):
        """Para o loop da CLI de fora (ex.: ao receber sinal do SO)."""
        self.running = False

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _dispatch(self, raw: str):
        """Analisa a linha digitada e chama o handler correto."""
        try:
            parts = shlex.split(raw)
        except ValueError as e:
            print(f"[CLI] Erro ao interpretar comando: {e}")
            return

        if not parts:
            return

        cmd = parts[0].lower()
        args = parts[1:]

        handlers = {
            "/help":      self._cmd_help,
            "/peers":     self._cmd_peers,
            "/msg":       self._cmd_msg,
            "/pub":       self._cmd_pub,
            "/conn":      self._cmd_conn,
            "/rtt":       self._cmd_rtt,
            "/reconnect": self._cmd_reconnect,
            "/log":       self._cmd_log,
            "/quit":      self._cmd_quit,
        }

        handler = handlers.get(cmd)
        if handler:
            handler(args)
        else:
            print(f"[CLI] Comando desconhecido: '{cmd}'. Digite /help para ajuda.")
            logger.warning("Comando desconhecido recebido: %s", cmd)

    # ------------------------------------------------------------------
    # Handlers individuais
    # ------------------------------------------------------------------

    def _cmd_help(self, args):
        print(HELP_TEXT)

    def _cmd_peers(self, args):
        """
        /peers           → lista todos os peers
        /peers *         → broadcast scope
        /peers #ns       → filtra pelo namespace
        """
        scope = args[0] if args else None
        # logger.info("Listando peers (escopo=%s)", scope or "todos")
        try:
            self.client.get_peers(scope)
        except Exception as e:
            print(f"[CLI] Erro ao listar peers: {e}")
            logger.error("Erro em /peers: %s", e)

    def _cmd_msg(self, args):
        """
        /msg <peer_id> <mensagem...>
        Exemplo: /msg bob@CIC Olá, tudo bem?
        """
        if len(args) < 2:
            print("[CLI] Uso: /msg <peer_id> <mensagem>")
            return

        peer_id = args[0]
        text = " ".join(args[1:])
        logger.info("Enviando SEND para %s", peer_id)

        try:
            self.client.send_message(peer_id, text)
        except Exception as e:
            print(f"[CLI] Erro ao enviar mensagem: {e}")
            logger.error("Erro em /msg para %s: %s", peer_id, e)

    def _cmd_pub(self, args):
        """
        /pub * <mensagem>           → broadcast global
        /pub #<namespace> <mensagem> → namespace-cast
        Exemplo: /pub #CIC Aviso para todos!
        """
        if len(args) < 2:
            print("[CLI] Uso: /pub <* | #namespace> <mensagem>")
            return

        dst = args[0]
        text = " ".join(args[1:])

        if dst != "*" and not dst.startswith("#"):
            print("[CLI] Destino inválido. Use '*' para broadcast ou '#namespace' para namespace-cast.")
            return

        scope_label = "broadcast global" if dst == "*" else f"namespace {dst}"
        logger.info("Publicando PUB para %s", dst)

        try:
            self.client.publish(dst, text)
        except Exception as e:
            print(f"[CLI] Erro ao publicar mensagem: {e}")
            logger.error("Erro em /pub para %s: %s", dst, e)

    def _cmd_conn(self, args):
        """Exibe conexões ativas (inbound e outbound)."""
        logger.info("Exibindo conexões ativas")
        try:
            print(self.client.show_all_connections())
        except Exception as e:
            print(f"[CLI] Erro ao exibir conexões: {e}")
            logger.error("Erro em /conn: %s", e)

    def _cmd_rtt(self, args):
        """Exibe RTT médio por peer."""
        logger.info("Exibindo RTT médio por peer")
        try:
            self.client.show_all_rtt()
        except Exception as e:
            print(f"[CLI] Erro ao exibir RTT: {e}")
            logger.error("Erro em /rtt: %s", e)

    def _cmd_reconnect(self, args):
        """Força reconciliação manual com os peers."""
        logger.info("Forçando reconciliação de peers")
        print("[CLI] Iniciando reconciliação de peers...")
        try:
            self.client.reconnect()
        except Exception as e:
            print(f"[CLI] Erro ao reconectar: {e}")
            logger.error("Erro em /reconnect: %s", e)

    def _cmd_log(self, args):
        """
        /log <NIVEL>
        Exemplo: /log DEBUG
        """
        if not args:
            current = logging.getLevelName(logging.getLogger().level)
            print(f"[CLI] Nível de log atual: {current}")
            print(f"[CLI] Níveis disponíveis: {', '.join(LOG_LEVELS.keys())}")
            return

        level_str = args[0].upper()
        if level_str not in LOG_LEVELS:
            print(f"[CLI] Nível inválido. Use: {', '.join(LOG_LEVELS.keys())}")
            return

        level = LOG_LEVELS[level_str]
        root = logging.getLogger()
        root.setLevel(level)
        for handler in root.handlers:
            handler.setLevel(level)
        logging.getLogger("P2PClient").setLevel(level)
        print(f"[CLI] Nível de log alterado para: {level_str}")
        logger.info("Nível de log alterado para %s", level_str)

    def _cmd_quit(self, args):
        """Encerra a aplicação de forma limpa."""
        print("[CLI] Encerrando aplicação...")
        logger.info("Usuário solicitou encerramento via /quit")
        self.running = False
        try:
            self.client.shutdown()
        except Exception as e:
            logger.error("Erro durante shutdown: %s", e)
