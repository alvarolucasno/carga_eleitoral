# -*- coding: utf-8 -*-
"""Logging padronizado para todos os scripts do projeto."""

import logging
import sys


def configurar_log(nome, nivel=logging.INFO):
    """Configura root logger + retorna logger nomeado com formato padronizado."""
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(nivel)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        root.addHandler(handler)

    logger = logging.getLogger(nome)
    logger.setLevel(nivel)
    return logger
