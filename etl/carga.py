"""FASE 3 - CARGA: serialización en memoria (io.BytesIO) y envío a AWS S3,
PostgreSQL o una carpeta local que replica las zonas del Data Lake."""

import io
import json
import logging
import re
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Serialización en memoria
# ----------------------------------------------------------------------
def a_csv_bytes(df):
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False, encoding="utf-8")
    return buffer.getvalue()


def a_parquet_bytes(df, compresion="snappy"):
    """Serializa el DataFrame a Parquet en memoria, sin escribir en disco."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression=compresion)
    buffer.seek(0)
    return buffer.getvalue()


def describir_parquet(contenido):
    """Metadatos del archivo Parquet: grupos de filas, compresión y esquema."""
    meta = pq.ParquetFile(io.BytesIO(contenido)).metadata
    columnas = [meta.row_group(0).column(i) for i in range(meta.num_columns)]
    return {
        "filas": meta.num_rows,
        "columnas": meta.num_columns,
        "row_groups": meta.num_row_groups,
        "compresion": columnas[0].compression,
        "creado_por": meta.created_by,
        "esquema": [(c.path_in_schema, c.physical_type) for c in columnas],
    }


# ----------------------------------------------------------------------
# AWS S3
# ----------------------------------------------------------------------
PATRON_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def verificar_aws(region, bucket):
    """Comprueba el nombre del bucket y que las credenciales de AWS estén vigentes.

    head_bucket no devuelve el motivo de un error (solo '400' o '403'), así que
    antes se valida el nombre y se consulta la identidad con STS, que sí dice
    si el token de AWS Academy expiró.
    """
    if not bucket or not PATRON_BUCKET.match(bucket) or ".." in bucket:
        raise ValueError(f"Nombre de bucket inválido en config.yaml: '{bucket}'. Usa solo minúsculas, "
                         "números, puntos y guiones (3 a 63 caracteres), sin espacios, acentos ni '_'.")
    sts = boto3.client("sts", region_name=region)
    try:
        identidad = sts.get_caller_identity()
    except ClientError as e:
        codigo = e.response["Error"]["Code"]
        if codigo in ("ExpiredToken", "ExpiredTokenException", "RequestExpired"):
            raise PermissionError("Las credenciales de AWS Academy EXPIRARON. Inicia el Learner Lab, "
                                  "copia de nuevo AWS Details -> AWS CLI en ~/.aws/credentials y reintenta.")
        if codigo in ("InvalidClientTokenId", "SignatureDoesNotMatch"):
            raise PermissionError("Las credenciales de ~/.aws/credentials no son válidas: copia el bloque "
                                  "completo (incluido aws_session_token) desde AWS Details -> AWS CLI.")
        raise
    log.info("Credenciales de AWS vigentes (cuenta %s)", identidad["Account"])


def asegurar_bucket(s3, bucket, region):
    """Crea el bucket si todavía no existe."""
    try:
        s3.head_bucket(Bucket=bucket)
        log.info("Bucket existente: %s", bucket)
    except ClientError as e:
        codigo = e.response["Error"]["Code"]
        if codigo == "403":
            raise PermissionError(f"El bucket '{bucket}' ya existe y pertenece a otra cuenta de AWS "
                                  "(los nombres son únicos en todo AWS). Cambia 'bucket' en config.yaml.")
        if codigo not in ("404", "NoSuchBucket"):
            raise
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
        log.info("Bucket creado: %s", bucket)


def subir_s3(s3, bucket, key, contenido, content_type):
    """put_object del contenido en memoria y verificación con head_object."""
    s3.put_object(Bucket=bucket, Key=key, Body=contenido, ContentType=content_type)
    info = s3.head_object(Bucket=bucket, Key=key)
    log.info("Subido s3://%s/%s (%s bytes)", bucket, key, f"{info['ContentLength']:,}")
    return {"key": key, "bytes": info["ContentLength"], "etag": info["ETag"].strip('"'),
            "fecha": info["LastModified"].isoformat()}


def leer_parquet_s3(s3, bucket, key):
    """Vuelve a descargar el Parquet para comprobar que se puede leer."""
    cuerpo = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(cuerpo))


def listar_s3(s3, bucket, prefijo=""):
    """Lista los objetos del bucket (equivalente a la vista de la consola)."""
    paginas = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefijo)
    return [(o["Key"], o["Size"]) for p in paginas for o in p.get("Contents", [])]


# ----------------------------------------------------------------------
# Alternativa local (misma estructura de zonas, sin AWS)
# ----------------------------------------------------------------------
def guardar_local(carpeta_base, key, contenido):
    ruta = Path(carpeta_base) / key
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_bytes(contenido)
    log.info("Guardado %s (%s bytes)", ruta, f"{len(contenido):,}")
    return {"key": str(ruta), "bytes": len(contenido)}


# ----------------------------------------------------------------------
# PostgreSQL
# ----------------------------------------------------------------------
def cargar_postgres(engine, df, esquema, tabla):
    """Carga el dataset procesado en una tabla analítica de PostgreSQL."""
    datos = df.copy()
    for c in datos.select_dtypes("category").columns:
        datos[c] = datos[c].astype(str)
    with engine.begin() as con:
        con.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {esquema}")
        datos.to_sql(tabla, con, schema=esquema, if_exists="replace", index=False,
                     method="multi", chunksize=2000)
    log.info("Cargado en PostgreSQL %s.%s: %s filas", esquema, tabla, f"{len(datos):,}")


def metadatos_json(**datos):
    return json.dumps(datos, ensure_ascii=False, indent=2, default=str).encode("utf-8")
