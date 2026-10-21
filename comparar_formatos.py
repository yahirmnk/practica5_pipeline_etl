"""
Comparación CSV vs. Parquet para el dataset procesado del pipeline.

Mide, en memoria (io.BytesIO), el tamaño, el tiempo de escritura, el tiempo de
lectura completa y el tiempo de leer solo 2 columnas, con distintas
compresiones. Repite la prueba con 200,000 clientes nuevos (generados con la
misma lógica que la fuente) para ver cómo escala. Guarda una tabla CSV y una gráfica en salidas/.

Uso:
    python comparar_formatos.py
"""

import io
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from etl import extraccion, transformacion
from etl.config import RAIZ, cargar_config, crear_engine
from preparar_fuente import generar_clientes

REPETICIONES = 5
COLUMNAS_CONSULTA = ["segmento_monto", "score_fidelidad"]
FORMATOS = {
    "CSV":              dict(tipo="csv", compression=None),
    "CSV + gzip":       dict(tipo="csv", compression="gzip"),
    "Parquet snappy":   dict(tipo="parquet", compression="snappy"),
    "Parquet gzip":     dict(tipo="parquet", compression="gzip"),
    "Parquet zstd":     dict(tipo="parquet", compression="zstd"),
}


def escribir(df, f):
    buffer = io.BytesIO()
    if f["tipo"] == "csv":
        df.to_csv(buffer, index=False, compression=f["compression"])
    else:
        df.to_parquet(buffer, index=False, compression=f["compression"])
    return buffer.getvalue()


def leer(contenido, f, columnas=None):
    buffer = io.BytesIO(contenido)
    if f["tipo"] == "csv":
        return pd.read_csv(buffer, usecols=columnas, compression=f["compression"] or "infer")
    return pd.read_parquet(buffer, columns=columnas)


def medir(funcion):
    tiempos = []
    for _ in range(REPETICIONES):
        t0 = time.perf_counter()
        resultado = funcion()
        tiempos.append(time.perf_counter() - t0)
    return min(tiempos) * 1000, resultado     # mejor de N, en milisegundos


def comparar(df, etiqueta):
    filas = []
    for nombre, f in FORMATOS.items():
        t_escritura, contenido = medir(lambda: escribir(df, f))
        t_lectura, leido = medir(lambda: leer(contenido, f))
        t_columnas, _ = medir(lambda: leer(contenido, f, COLUMNAS_CONSULTA))
        tipos_ok = (leido["fecha_registro"].dtype.kind == "M") and (str(leido["categoria_edad"].dtype) == "category")
        filas.append({"dataset": etiqueta, "formato": nombre, "tamano_kb": len(contenido) / 1024,
                      "escritura_ms": t_escritura, "lectura_ms": t_lectura, "lectura_2_columnas_ms": t_columnas,
                      "conserva_tipos": "Sí" if tipos_ok else "No"})
    tabla = pd.DataFrame(filas)
    base = tabla.loc[tabla["formato"] == "CSV", "tamano_kb"].iloc[0]
    tabla["pct_vs_csv"] = tabla["tamano_kb"] / base * 100
    return tabla


def graficar(tabla, ruta):
    fig, ejes = plt.subplots(1, 3, figsize=(16, 4.6))
    for ax, (col, titulo) in zip(ejes, [("tamano_kb", "Tamaño (KB)"),
                                        ("lectura_ms", "Lectura completa (ms)"),
                                        ("lectura_2_columnas_ms", "Lectura de 2 columnas (ms)")]):
        colores = ["#9E9E9E", "#BDBDBD", "#1F4E9A", "#4F7CC4", "#8FB0E0"]
        barras = ax.bar(tabla["formato"], tabla[col], color=colores, edgecolor="black")
        ax.bar_label(barras, fmt="%.0f", fontsize=8, padding=2)
        ax.set_title(titulo)
        ax.tick_params(axis="x", rotation=30, labelsize=8)
    filas = tabla.attrs.get("filas", "")
    fig.suptitle(f"CSV vs. Parquet · dataset procesado ({filas:,} filas)")
    fig.tight_layout()
    fig.savefig(ruta, dpi=150, bbox_inches="tight")


def main():
    cfg = cargar_config()
    p = cfg["pipeline"]
    engine = crear_engine(cfg["postgres"])
    df_raw = extraccion.extraer_postgres(engine, p["esquema_origen"], p["tabla_origen"])
    df, _, _ = transformacion.transformar(df_raw)

    salidas = RAIZ / "salidas"
    salidas.mkdir(exist_ok=True)
    # Dataset grande: 200,000 clientes NUEVOS (no copias: duplicar filas
    # exageraría artificialmente la compresión)
    grande, _, _ = transformacion.transformar(generar_clientes(200_000, semilla=7))

    resultados = []
    for datos, etiqueta in [(df, f"{len(df):,} filas"), (grande, f"{len(grande):,} filas")]:
        tabla = comparar(datos, etiqueta)
        tabla.attrs["filas"] = len(datos)
        resultados.append(tabla)
        print(f"\n=== Dataset procesado: {etiqueta} ===")
        print(tabla.drop(columns="dataset").to_string(index=False, float_format=lambda v: f"{v:,.1f}"))

    graficar(resultados[1], salidas / "comparacion_csv_parquet.png")
    pd.concat(resultados).to_csv(salidas / "comparacion_csv_parquet.csv", index=False, float_format="%.2f")
    print(f"\nResultados guardados en {salidas}")


if __name__ == "__main__":
    main()
