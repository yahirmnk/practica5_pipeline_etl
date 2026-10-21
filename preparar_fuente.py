"""
Prepara la FUENTE del pipeline: crea en PostgreSQL la base de datos, el esquema
'operaciones' y la tabla 'clientes', que simula el sistema operacional (OLTP)
de una tienda. Se ejecuta UNA vez antes del pipeline.

Los datos incluyen problemas de calidad a propósito (nulos, montos negativos,
edades imposibles) para que la fase de transformación los trate.

Uso:
    python preparar_fuente.py              # 20,000 clientes
    python preparar_fuente.py --filas 50000
"""

import argparse

import numpy as np
import pandas as pd
from sqlalchemy import text

from etl.config import cargar_config, crear_engine

SEMILLA = 2026
CIUDADES = ["CDMX", "Guadalajara", "Monterrey", "Puebla", "Querétaro", "Mérida", "Tijuana", "León"]
CANALES = ["Tienda física", "Sitio web", "App móvil"]


def generar_clientes(n, semilla=SEMILLA):
    rng = np.random.default_rng(semilla)
    edad = rng.normal(40, 14, n).round().clip(18, 90)
    num_compras = rng.poisson(12, n) + 1
    # Monto con sesgo a la derecha (lognormal): muchos clientes con montos
    # moderados y pocos con montos muy altos, como en la realidad.
    ticket = rng.lognormal(mean=6.6, sigma=0.7, size=n)
    monto = (ticket * num_compras).round(2)
    df = pd.DataFrame({
        "id_cliente": np.arange(1, n + 1),
        "cliente": [f"CLI-{i:06d}" for i in range(1, n + 1)],
        "edad": edad,
        "ciudad": rng.choice(CIUDADES, n, p=[0.25, 0.15, 0.15, 0.12, 0.1, 0.08, 0.08, 0.07]),
        "canal_preferido": rng.choice(CANALES, n, p=[0.5, 0.3, 0.2]),
        "fecha_registro": pd.Timestamp("2018-01-01") + pd.to_timedelta(rng.integers(0, 2922, n), unit="D"),
        "num_compras": num_compras,
        "monto_compras": monto,
    })
    # Problemas de calidad intencionales
    idx = rng.permutation(n)
    df.loc[idx[:int(n * 0.02)], "edad"] = np.nan                                  # 2 % edad nula
    df.loc[idx[int(n * 0.02):int(n * 0.025)], "edad"] = 150                       # 0.5 % edad imposible
    df.loc[idx[int(n * 0.025):int(n * 0.03)], "monto_compras"] = np.nan           # 0.5 % monto nulo
    neg = idx[int(n * 0.03):int(n * 0.033)]
    df.loc[neg, "monto_compras"] = -df.loc[neg, "monto_compras"]                  # 0.3 % monto negativo
    return df


DDL = """
CREATE SCHEMA IF NOT EXISTS {e};
DROP TABLE IF EXISTS {e}.{t};
CREATE TABLE {e}.{t} (
    id_cliente       INTEGER PRIMARY KEY,
    cliente          VARCHAR(20)   NOT NULL,
    edad             SMALLINT,
    ciudad           VARCHAR(40)   NOT NULL,
    canal_preferido  VARCHAR(20)   NOT NULL,
    fecha_registro   DATE          NOT NULL,
    num_compras      INTEGER       NOT NULL,
    monto_compras    NUMERIC(14,2)
);
"""


def main(filas):
    cfg = cargar_config()
    pg, p = cfg["postgres"], cfg["pipeline"]

    # Crear la base de datos si no existe
    engine_admin = crear_engine(pg, database="postgres").execution_options(isolation_level="AUTOCOMMIT")
    with engine_admin.connect() as con:
        if not con.execute(text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": pg["database"]}).scalar():
            con.execute(text(f'CREATE DATABASE "{pg["database"]}"'))
            print(f"Base de datos creada: {pg['database']}")
    engine_admin.dispose()

    df = generar_clientes(filas)
    engine = crear_engine(pg)
    with engine.begin() as con:
        con.exec_driver_sql(DDL.format(e=p["esquema_origen"], t=p["tabla_origen"]))
        datos = df.copy()
        datos["fecha_registro"] = datos["fecha_registro"].dt.date
        datos["edad"] = datos["edad"].astype("Int16")
        datos.to_sql(p["tabla_origen"], con, schema=p["esquema_origen"], if_exists="append",
                     index=False, method="multi", chunksize=2000)
        total = con.execute(text(f"SELECT COUNT(*) FROM {p['esquema_origen']}.{p['tabla_origen']}")).scalar()
    print(f"Tabla fuente {p['esquema_origen']}.{p['tabla_origen']} creada con {total:,} clientes")
    print(df.head().to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crea la tabla fuente (OLTP) en PostgreSQL")
    parser.add_argument("--filas", type=int, default=20000)
    main(parser.parse_args().filas)
