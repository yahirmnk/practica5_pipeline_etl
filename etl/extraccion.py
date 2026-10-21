"""FASE 1 - EXTRACCIÓN: lectura de los datos operativos de origen."""

import io
import logging

import pandas as pd
from sqlalchemy import text

log = logging.getLogger(__name__)

COLUMNAS = ["id_cliente", "cliente", "edad", "ciudad", "canal_preferido",
            "fecha_registro", "num_compras", "monto_compras"]


def extraer_postgres(engine, esquema, tabla):
    """Extrae los clientes del sistema operacional (OLTP) con una consulta SQL."""
    consulta = text(f"""
        SELECT {", ".join(COLUMNAS)}
        FROM {esquema}.{tabla}
        ORDER BY id_cliente
    """)
    with engine.connect() as con:
        df = pd.read_sql(consulta, con, parse_dates=["fecha_registro"])
    log.info("Extraídos %s registros de PostgreSQL (%s.%s)", f"{len(df):,}", esquema, tabla)
    return df


def extraer_csv_s3(s3, bucket, key):
    """Fuente alternativa: lee un CSV de la zona raw de S3 directamente en memoria."""
    respuesta = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(respuesta["Body"].read()), parse_dates=["fecha_registro"])
    log.info("Extraídos %s registros de s3://%s/%s", f"{len(df):,}", bucket, key)
    return df


def perfil_rapido(df):
    """Resumen de calidad de los datos extraídos."""
    return {
        "registros": len(df),
        "columnas": df.shape[1],
        "nulos_por_columna": {c: int(n) for c, n in df.isna().sum().items() if n},
        "ids_duplicados": int(df["id_cliente"].duplicated().sum()),
        "montos_negativos": int((df["monto_compras"] < 0).sum()),
        "edades_fuera_de_rango": int(((df["edad"] < 18) | (df["edad"] > 100)).sum()),
    }
