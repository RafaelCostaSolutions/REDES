"""
logger.py — Configuração centralizada do sistema de logs
Todos os módulos importam logging e usam logging.getLogger(__name__).
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logging(log_level: str = "INFO", log_to_file: bool = False, log_dir: str = "logs"):
    """
    Configura o sistema de logging global da aplicação.

    Parâmetros:
        log_level   — Nível mínimo de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file — Se True, também salva logs em arquivo no diretório log_dir
        log_dir     — Pasta onde os arquivos de log serão salvos
    """

    level = getattr(logging, log_level.upper(), logging.INFO)

    # Formato: [2025-10-27 10:00:00] [INFO] [NomeDoModulo] mensagem
    fmt = "[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=date_fmt)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove handlers antigos para evitar duplicação ao chamar setup de novo
    root_logger.handlers.clear()

    # --- Handler para o terminal (stdout) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # --- Handler para arquivo (opcional) ---
    if log_to_file:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_path / f"pyp2p_{timestamp}.log"

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)   # arquivo sempre guarda tudo
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        logging.getLogger("Logger").info(
            "Logs sendo salvos em: %s", log_file
        )

    logging.getLogger("Logger").info(
        "Sistema de logging iniciado. Nível: %s", log_level.upper()
    )
