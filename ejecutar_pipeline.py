"""
Práctica 5: Pipeline ETL Automatizado a AWS S3 / PostgreSQL.

    EXTRACT   PostgreSQL (operaciones.clientes)  ->  zona RAW de S3 (CSV)
    TRANSFORM limpieza, pd.cut / pd.qcut, MinMaxScaler, feature engineering
    LOAD      Parquet en memoria (io.BytesIO) -> zona PROCESSED de S3
              + rechazados -> zona QUARANTINE + metadatos JSON
              + tabla analitica.clientes_features en PostgreSQL

Uso:
    python ejecutar_pipeline.py                       # fuente PostgreSQL, destino S3 + PostgreSQL
    python ejecutar_pipeline.py --fuente s3           # reprocesa el CSV de la zona raw de S3
    python ejecutar_pipeline.py --local               # sin AWS: zonas en la carpeta data_lake/
    python ejecutar_pipeline.py --sin-postgres-destino
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from sqlalchemy.exc import OperationalError

from etl import carga, extraccion, transformacion
from etl.config import RAIZ, cargar_config, crear_cliente_s3, crear_engine

pd.set_option("display.width", 170)
pd.set_option("display.max_columns", 25)
pd.set_option("display.float_format", lambda v: f"{v:,.4f}")


def configurar_logging():
    carpeta = RAIZ / "logs"
    carpeta.mkdir(exist_ok=True)
    archivo = carpeta / f"pipeline_{datetime.now():%Y%m%d}.log"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                        handlers=[logging.FileHandler(archivo, encoding="utf-8"), logging.StreamHandler(sys.stdout)])
    return logging.getLogger("pipeline")


def titulo(texto):
    print(f"\n{'=' * 78}\n{texto}\n{'=' * 78}")


def pipeline_etl_completo(fuente="postgres", local=False, postgres_destino=True):
    log = configurar_logging()
    cfg = cargar_config()
    p = cfg["pipeline"]
    fecha_carga = datetime.now().strftime("%Y-%m-%d")
    inicio = time.perf_counter()
    tiempos = {}

    # Destino: S3 real o carpeta local con las mismas zonas
    s3 = None if local else crear_cliente_s3(cfg["aws"]["region"])
    bucket = cfg["aws"].get("bucket")

    def guardar(key, contenido, tipo):
        if local:
            return carga.guardar_local(RAIZ / "data_lake", key, contenido)
        return carga.subir_s3(s3, bucket, key, contenido, tipo)

    key_raw = f"{p['prefijo_raw']}/fecha_carga={fecha_carga}/clientes.csv"
    key_parquet = f"{p['prefijo_processed']}/fecha_carga={fecha_carga}/clientes_features.parquet"
    key_meta = f"{p['prefijo_processed']}/fecha_carga={fecha_carga}/_metadata.json"
    key_rechazados = f"{p['prefijo_quarantine']}/fecha_carga={fecha_carga}/rechazados.csv"

    try:
        if not local:
            carga.verificar_aws(cfg["aws"]["region"], bucket)
            carga.asegurar_bucket(s3, bucket, cfg["aws"]["region"])

        # ============================== EXTRACT ==============================
        titulo("=== 1. FASE DE EXTRACCIÓN (Extract) ===")
        t0 = time.perf_counter()
        if fuente == "postgres":
            engine = crear_engine(cfg["postgres"])
            df_raw = extraccion.extraer_postgres(engine, p["esquema_origen"], p["tabla_origen"])
            info_raw = guardar(key_raw, carga.a_csv_bytes(df_raw), "text/csv")      # aterriza en RAW
        else:
            if local:
                df_raw = pd.read_csv(RAIZ / "data_lake" / key_raw, parse_dates=["fecha_registro"])
            else:
                df_raw = extraccion.extraer_csv_s3(s3, bucket, key_raw)
            info_raw = {"key": key_raw, "bytes": None}
        tiempos["extraccion"] = time.perf_counter() - t0
        print(df_raw.head().to_string(index=False))
        perfil = extraccion.perfil_rapido(df_raw)
        print(f"\nPerfil de calidad de origen: {perfil}")

        # ============================= TRANSFORM =============================
        titulo("=== 2. FASE DE TRANSFORMACIÓN (Transform) ===")
        t0 = time.perf_counter()
        df_proc, df_rech, parametros = transformacion.transformar(df_raw)
        tiempos["transformacion"] = time.perf_counter() - t0

        print("Dataset Transformado (primeras filas):")
        print(df_proc.head().to_string(index=False))
        print("\nParámetros aprendidos por MinMaxScaler (x' = (x - min) / (max - min)):")
        for col, v in parametros.items():
            print(f"  {col:<22} min = {v['min']:>14,.4f}   max = {v['max']:>14,.4f}")
        print("\nDistribución de categoria_edad (pd.cut):")
        print(df_proc["categoria_edad"].value_counts().reindex(
            transformacion.ETIQUETAS_EDAD + ["Desconocido"]).to_string())
        print("\nLímites de segmento_monto (pd.qcut, cuartiles):")
        print(df_proc.groupby("segmento_monto", observed=True)["monto_compras"]
              .agg(["count", "min", "max"]).to_string())
        print("\nResumen de variables normalizadas y scores:")
        print(df_proc[["monto_normalizado", "monto_log_normalizado", "num_compras_normalizado",
                       "antiguedad_dias_normalizado", "score_promocional", "score_fidelidad"]]
              .describe().T[["mean", "50%", "min", "max"]].to_string())
        print(f"\nRegistros rechazados (cuarentena): {len(df_rech)}")
        print(df_rech["motivo_rechazo"].value_counts().to_string())

        # =============================== LOAD ================================
        titulo("=== 3. FASE DE CARGA (Load) ===")
        t0 = time.perf_counter()
        parquet = carga.a_parquet_bytes(df_proc, compresion="snappy")
        csv_equivalente = carga.a_csv_bytes(df_proc)
        print(f"Parquet en memoria: {len(parquet):,} bytes | mismo dataset en CSV: {len(csv_equivalente):,} bytes "
              f"({len(parquet) / len(csv_equivalente):.1%} del tamaño)")
        meta_parquet = carga.describir_parquet(parquet)
        print(f"Metadatos Parquet: {meta_parquet['filas']:,} filas, {meta_parquet['columnas']} columnas, "
              f"{meta_parquet['row_groups']} row group(s), compresión {meta_parquet['compresion']}")

        info_parquet = guardar(key_parquet, parquet, "application/vnd.apache.parquet")
        info_rech = guardar(key_rechazados, carga.a_csv_bytes(df_rech), "text/csv")
        guardar(key_meta, carga.metadatos_json(
            fecha_carga=fecha_carga, fuente=fuente, filas_origen=len(df_raw), filas_procesadas=len(df_proc),
            filas_rechazadas=len(df_rech), parametros_minmax=parametros, columnas=list(df_proc.columns),
            bins_edad=transformacion.BINS_EDAD, etiquetas_edad=transformacion.ETIQUETAS_EDAD,
            parquet=meta_parquet), "application/json")

        if postgres_destino:
            engine = crear_engine(cfg["postgres"])
            carga.cargar_postgres(engine, df_proc, p["esquema_destino"], p["tabla_destino"])
        tiempos["carga"] = time.perf_counter() - t0

        # =========================== VERIFICACIÓN ============================
        titulo("=== 4. VERIFICACIÓN ===")
        if local:
            relectura = pd.read_parquet(RAIZ / "data_lake" / key_parquet)
            objetos = sorted((str(r.relative_to(RAIZ / "data_lake")).replace("\\", "/"), r.stat().st_size)
                             for r in (RAIZ / "data_lake").rglob("*") if r.is_file())
            destino = f"carpeta local {RAIZ / 'data_lake'}"
        else:
            relectura = carga.leer_parquet_s3(s3, bucket, key_parquet)
            objetos = carga.listar_s3(s3, bucket)
            destino = f"s3://{bucket}"
        print(f"Objetos en {destino}:")
        for key, tam in objetos:
            print(f"  {tam:>12,} bytes  {key}")
        coincide = relectura.shape == df_proc.shape
        print(f"\nRelectura del Parquet: {relectura.shape} -> {'OK, coincide' if coincide else 'NO coincide'}")
        print(f"Tipos conservados por Parquet: { {k: int(v) for k, v in relectura.dtypes.astype(str).value_counts().items()} }")

        total = time.perf_counter() - inicio
        titulo("RESUMEN DEL PIPELINE")
        print(f"Fuente: {fuente} | Destino: {destino} | Fecha de carga: {fecha_carga}")
        print(f"Filas: origen {len(df_raw):,} -> procesadas {len(df_proc):,} + rechazadas {len(df_rech):,}")
        print(f"RAW:        {info_raw['key']}")
        print(f"PROCESSED:  {info_parquet['key']} ({info_parquet['bytes']:,} bytes)")
        print(f"QUARANTINE: {info_rech['key']}")
        print("Tiempos: " + " | ".join(f"{k} {v:.2f} s" for k, v in tiempos.items()) + f" | total {total:.2f} s")
        print(f"-> Éxito: archivo Parquet comprimido cargado en la zona 'processed' ({key_parquet}).")
        log.info("Pipeline terminado correctamente en %.2f s", total)

    except NoCredentialsError:
        log.error("No hay credenciales de AWS. Copia las de AWS Academy en ~/.aws/credentials "
                  "o ejecuta con --local.")
        sys.exit(1)
    except ClientError as e:
        codigo = e.response["Error"]["Code"]
        ayuda = {"ExpiredToken": "La sesión de AWS Academy expiró: actualiza ~/.aws/credentials.",
                 "AccessDenied": "Acceso denegado al bucket.",
                 "BucketAlreadyExists": "Ese nombre de bucket ya lo usa otra cuenta: cambia 'bucket' en config.yaml.",
                 "NoSuchKey": "No existe el CSV en la zona raw: ejecuta primero con --fuente postgres.",
                 "400": "Solicitud inválida: normalmente el token de AWS Academy expiró o la región no es us-east-1."}
        log.error("Error de AWS [%s]: %s", codigo, ayuda.get(codigo, e))
        sys.exit(1)
    except (PermissionError, ValueError) as e:
        log.error("%s", e)
        sys.exit(1)
    except OperationalError as e:
        log.error("No se pudo conectar a PostgreSQL: revisa que el servicio esté iniciado y los datos de "
                  "config.yaml. Detalle: %s", str(e.orig).strip())
        sys.exit(1)
    except UnicodeDecodeError:
        log.error("PostgreSQL rechazó la conexión (en Windows en español el mensaje no se puede leer): "
                  "revisa usuario y contraseña en config.yaml.")
        sys.exit(1)
    except Exception:
        log.exception("El pipeline falló")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline ETL: PostgreSQL -> S3 (Raw/Processed) en Parquet")
    parser.add_argument("--fuente", choices=["postgres", "s3"], default="postgres")
    parser.add_argument("--local", action="store_true", help="guardar las zonas en data_lake/ en lugar de S3")
    parser.add_argument("--sin-postgres-destino", action="store_true", help="no cargar la tabla analítica")
    args = parser.parse_args()
    pipeline_etl_completo(args.fuente, args.local, not args.sin_postgres_destino)
