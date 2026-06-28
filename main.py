"""
main.py — Ponto de entrada da aplicação PyP2P
Responsável por:
  1. Carregar configurações (config.json)
  2. Inicializar o sistema de logging
  3. Instanciar e iniciar o cliente P2P
  4. Iniciar a CLI em thread separada
  5. Aguardar encerramento e limpar recursos
  Grupo 4 
  Membros: Rafael Babrosa da Costa - 190058811
  André Greff Costa - 231012700
  Pedro Augusto Martins dos Santos - 242024692
  Disciplina de Redes de Computadores
"""

import json
import logging
import signal
import sys
import threading
from pathlib import Path

from p2p_client import P2PClient
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
    """
    Valida tipos e valores de config.json antes de qualquer módulo ser
    instanciado. Sem isso, um campo com tipo errado (ex.: porta como string)
    só quebra minutos depois, dentro de uma thread, com traceback confuso.

    Retorna True se tudo estiver válido; caso contrário, loga cada erro
    encontrado (não para no primeiro) e retorna False.
    """
    ok = True

    def fail(msg, *args):
        nonlocal ok
        logger.error("Config inválida: " + msg, *args)
        ok = False

    # --- Campos obrigatórios presentes e não vazios ---
    required = ["peer_id", "rendezvous_host", "rendezvous_port", "listen_port"]
    for field in required:
        value = config.get(field)
        if value is None or value == "":
            fail("campo obrigatório ausente ou vazio: '%s'", field)

    if not ok:
        return False  # sem os obrigatórios, nem vale checar o resto

    # --- peer_id: precisa ser "name@namespace", sem partes vazias ---
    peer_id = config["peer_id"]
    if not isinstance(peer_id, str) or peer_id.count("@") != 1:
        fail("'peer_id' deve ser uma string no formato 'name@namespace' (recebido: %r)", peer_id)
    else:
        name, namespace = peer_id.split("@", 1)
        if not name or not namespace:
            fail("'peer_id' tem name ou namespace vazio (recebido: %r)", peer_id)

    # --- rendezvous_host: string não vazia ---
    if not isinstance(config["rendezvous_host"], str) or not config["rendezvous_host"].strip():
        fail("'rendezvous_host' deve ser uma string não vazia (recebido: %r)", config["rendezvous_host"])

    # --- Campos que precisam ser inteiros (bool é subclasse de int em Python,
    # então checamos explicitamente para não deixar passar true/false como porta) ---
    int_fields = {
        "rendezvous_port": (1, 65535),
        "listen_port": (1, 65535),
        "ping_interval": (1, None),
        "max_reconnect_attempts": (0, None),
    }

    for field, (min_val, max_val) in int_fields.items():
        value = config.get(field)
        is_valid_int = isinstance(value, int) and not isinstance(value, bool)

        if not is_valid_int:
            fail(
                "'%s' deve ser um número inteiro (recebido: %r, tipo: %s)",
                field, value, type(value).__name__
            )
            continue

        if value < min_val or (max_val is not None and value > max_val):
            range_desc = f">= {min_val}" if max_val is None else f"entre {min_val} e {max_val}"
            fail("'%s' deve estar %s (recebido: %s)", field, range_desc, value)

    # --- Campos booleanos ---
    bool_fields = ["log_to_file"]
    for field in bool_fields:
        value = config.get(field)
        if value is not None and not isinstance(value, bool):
            fail("'%s' deve ser true/false (recebido: %r, tipo: %s)", field, value, type(value).__name__)

    # --- log_level: precisa ser um nível conhecido ---
    valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    log_level = config.get("log_level", "INFO")
    if not isinstance(log_level, str) or log_level.upper() not in valid_levels:
        fail("'log_level' deve ser um dos seguintes: %s (recebido: %r)", ", ".join(valid_levels), log_level)

    # --- Checagem de sanidade extra: evita bind conflict óbvio em testes locais ---
    if (
        isinstance(config.get("listen_port"), int)
        and isinstance(config.get("rendezvous_port"), int)
        and config["listen_port"] == config["rendezvous_port"]
        and config.get("rendezvous_host") in ("localhost", "127.0.0.1")
    ):
        fail("'listen_port' e 'rendezvous_port' não podem ser iguais ao testar com rendezvous local")

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

    config["name"], config["namespace"] = config.get("peer_id").split("@")

    # 4. Instanciar cliente P2P
    #    Troque _P2PClientStub por P2PClient quando o módulo estiver pronto:
    #    client = P2PClient(config)
    client = P2PClient(config)

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
