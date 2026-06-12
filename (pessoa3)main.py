"""
main.py — Ponto de entrada da aplicação PyP2P
Responsável por:
  1. Carregar configurações (config.json)
  2. Inicializar o sistema de logging
  3. Instanciar e iniciar o cliente P2P
  4. Iniciar a CLI em thread separada
  5. Aguardar encerramento e limpar recursos
"""

import json
import logging
import signal
import sys
import threading
from pathlib import Path

from logger import setup_logging
from cli import CLI

# -----------------------------------------------------------------------
# Importações dos outros módulos (feitos pelas Pessoas 1 e 2)
# Descomente quando os módulos estiverem prontos:
# from p2p_client import P2PClient
# -----------------------------------------------------------------------

logger = logging.getLogger("Main")

DEFAULT_CONFIG = {
    "peer_id": "user@namespace",
    "rendezvous_host": "pyp2p.mfcaetano.cc",
    "rendezvous_port": 8080,
    "listen_port": 9000,
    "ping_interval": 30,
    "max_reconnect_attempts": 5,
    "log_level": "INFO",
    "log_to_file": False,
    "log_dir": "logs"
}


# -----------------------------------------------------------------------
# Stub do P2PClient — usado enquanto os outros módulos não estão prontos
# Substitua pela importação real quando disponível.
# -----------------------------------------------------------------------
class _P2PClientStub:
    """
    Stub temporário do P2PClient.
    Permite rodar e testar a CLI de forma isolada.
    Remove esta classe quando o p2p_client.py real estiver pronto.
    """

    def __init__(self, config):
        self.config = config
        self._peer_id = config.get("peer_id", "user@namespace")

    def start(self):
        logger.info("(Stub) P2PClient iniciado como %s", self._peer_id)

    def list_peers(self, scope):
        scope_label = scope or "todos"
        print(f"[Stub] Peers conhecidos (escopo={scope_label}): nenhum ainda.")

    def send_message(self, peer_id, text):
        print(f"[Stub] SEND → {peer_id}: {text}")

    def publish_message(self, dst, text):
        print(f"[Stub] PUB → {dst}: {text}")

    def show_connections(self):
        print("[Stub] Conexões ativas: nenhuma.")

    def show_rtt(self):
        print("[Stub] RTT: sem dados ainda.")

    def force_reconnect(self):
        print("[Stub] Reconciliação forçada.")

    def shutdown(self):
        logger.info("(Stub) P2PClient encerrado.")


# -----------------------------------------------------------------------
# Funções auxiliares
# -----------------------------------------------------------------------

def load_config(path: str = "config.json") -> dict:
    """
    Carrega config.json. Se não existir, cria um com valores padrão.
    Valores ausentes no arquivo são preenchidos com DEFAULT_CONFIG.
    """
    config_path = Path(path)

    if not config_path.exists():
        logger.warning("config.json não encontrado. Criando com valores padrão...")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
        return DEFAULT_CONFIG.copy()

    with open(config_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    # Preenche campos faltantes com os defaults
    config = {**DEFAULT_CONFIG, **loaded}
    return config


def validate_config(config: dict) -> bool:
    """Valida campos obrigatórios da configuração."""
    required = ["peer_id", "rendezvous_host", "rendezvous_port"]
    ok = True
    for field in required:
        if not config.get(field):
            logger.error("Campo obrigatório ausente em config.json: '%s'", field)
            ok = False
    return ok


# -----------------------------------------------------------------------
# Ponto de entrada
# -----------------------------------------------------------------------

def main():
    # 1. Carregar configuração
    config = load_config("config.json")

    # 2. Inicializar logging (antes de qualquer logger.xxx())
    setup_logging(
        log_level=config.get("log_level", "INFO"),
        log_to_file=config.get("log_to_file", False),
        log_dir=config.get("log_dir", "logs"),
    )

    logger.info("=== PyP2P iniciando ===")
    logger.info("Peer ID: %s", config["peer_id"])
    logger.info("Rendezvous: %s:%s", config["rendezvous_host"], config["rendezvous_port"])

    # 3. Validar configuração
    if not validate_config(config):
        logger.critical("Configuração inválida. Encerrando.")
        sys.exit(1)

    # 4. Instanciar cliente P2P
    #    Troque _P2PClientStub por P2PClient quando o módulo estiver pronto:
    #    client = P2PClient(config)
    client = _P2PClientStub(config)

    # 5. Iniciar o cliente em thread separada (não bloqueia a CLI)
    client_thread = threading.Thread(target=client.start, name="P2PClient", daemon=True)
    client_thread.start()

    # 6. Capturar Ctrl+C (SIGINT) para encerramento limpo
    def handle_signal(sig, frame):
        logger.info("Sinal %s recebido. Encerrando...", signal.Signals(sig).name)
        client.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # 7. Iniciar a CLI (bloqueia até /quit)
    cli = CLI(client)
    cli.run()

    # 8. Cleanup final
    logger.info("=== PyP2P encerrado ===")


if __name__ == "__main__":
    main()
