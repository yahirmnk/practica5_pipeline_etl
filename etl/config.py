"""Lectura de la configuración y creación de conexiones (PostgreSQL y S3).

Las credenciales de PostgreSQL y el nombre del bucket se leen de config.yaml,
que está excluido de Git. Las credenciales de AWS NO se guardan en el proyecto:
boto3 las toma de ~/.aws/credentials (AWS Academy).
"""

import sys
from pathlib import Path

import boto3
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

RAIZ = Path(__file__).resolve().parent.parent
ARCHIVO_CONFIG = RAIZ / "config.yaml"

VALORES_POR_DEFECTO = {
    "aws": {"region": "us-east-1"},
    "pipeline": {
        "esquema_origen": "operaciones",
        "tabla_origen": "clientes",
        "esquema_destino": "analitica",
        "tabla_destino": "clientes_features",
        "prefijo_raw": "raw/clientes",
        "prefijo_processed": "processed/clientes_features",
        "prefijo_quarantine": "quarantine/clientes",
    },
}


def cargar_config(ruta=ARCHIVO_CONFIG):
    """Lee config.yaml y completa los valores por defecto."""
    if not ruta.exists():
        sys.exit("ERROR: no existe config.yaml.\n"
                 "Copia config.example.yaml como config.yaml y escribe tus datos.")
    with open(ruta, encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    for seccion, valores in VALORES_POR_DEFECTO.items():
        cfg.setdefault(seccion, {})
        for clave, valor in valores.items():
            cfg[seccion].setdefault(clave, valor)
    return cfg


def crear_engine(cfg_pg, database=None):
    """Engine de SQLAlchemy para PostgreSQL."""
    url = URL.create("postgresql+psycopg2", username=cfg_pg["user"], password=str(cfg_pg["password"]),
                     host=cfg_pg["host"], port=cfg_pg["port"], database=database or cfg_pg["database"])
    return create_engine(url, connect_args={"client_encoding": "utf8"})


def crear_cliente_s3(region):
    """Cliente de S3; las credenciales vienen de ~/.aws/credentials."""
    return boto3.client("s3", region_name=region)
