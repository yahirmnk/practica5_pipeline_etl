"""FASE 2 - TRANSFORMACIÓN: limpieza, discretización, normalización e
ingeniería de características."""

import logging

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

log = logging.getLogger(__name__)

# Fecha fija para calcular antigüedad: así el resultado es reproducible
FECHA_REFERENCIA = pd.Timestamp("2026-01-01")

# Discretización del código base (pd.cut): intervalos (0, 25], (25, 50], (50, 100]
BINS_EDAD = [0, 25, 50, 100]
ETIQUETAS_EDAD = ["Joven", "Adulto", "Senior"]
ETIQUETAS_SEGMENTO = ["Bajo", "Medio", "Alto", "Premium"]


# ----------------------------------------------------------------------
# 1. Limpieza y validación
# ----------------------------------------------------------------------
def limpiar(df):
    """Separa los registros válidos de los rechazados (zona de cuarentena)."""
    df = df.drop_duplicates(subset="id_cliente").copy()
    motivo = pd.Series("", index=df.index)
    motivo[df["monto_compras"].isna()] += "monto nulo; "
    motivo[df["monto_compras"] < 0] += "monto negativo; "
    motivo[df["num_compras"].isna() | (df["num_compras"] <= 0)] += "num_compras inválido; "

    rechazados = df[motivo != ""].assign(motivo_rechazo=motivo[motivo != ""].str.rstrip("; "))
    validos = df[motivo == ""].copy()

    # Una edad faltante o imposible no invalida al cliente: se marca como desconocida
    edades_invalidas = validos["edad"].notna() & ~validos["edad"].between(18, 100)
    validos.loc[edades_invalidas, "edad"] = np.nan

    log.info("Limpieza: %s válidos, %s rechazados, %s edades inválidas -> NaN",
             f"{len(validos):,}", f"{len(rechazados):,}", int(edades_invalidas.sum()))
    return validos, rechazados


# ----------------------------------------------------------------------
# 2. Discretización (binning)
# ----------------------------------------------------------------------
def discretizar(df):
    """Generaliza variables continuas en categorías."""
    # Código base: rangos fijos definidos por el negocio
    df["categoria_edad"] = pd.cut(df["edad"], bins=BINS_EDAD, labels=ETIQUETAS_EDAD)
    df["categoria_edad"] = df["categoria_edad"].cat.add_categories("Desconocido").fillna("Desconocido")
    # Extensión: cuartiles (pd.qcut) -> cada segmento tiene ~25 % de los clientes
    df["segmento_monto"] = pd.qcut(df["monto_compras"], q=4, labels=ETIQUETAS_SEGMENTO)
    return df


# ----------------------------------------------------------------------
# 3. Normalización Min-Max
# ----------------------------------------------------------------------
def normalizar(df):
    """Escala las variables numéricas al rango [0, 1] con MinMaxScaler."""
    parametros = {}

    # Código base: monto_compras directamente
    scaler = MinMaxScaler()
    df["monto_normalizado"] = scaler.fit_transform(df[["monto_compras"]]).ravel()
    parametros["monto_compras"] = {"min": float(scaler.data_min_[0]), "max": float(scaler.data_max_[0])}

    # Extensión: el monto tiene sesgo fuerte (pocos clientes con montos muy altos),
    # así que también se normaliza su logaritmo para repartir mejor los valores.
    scaler_log = MinMaxScaler()
    df["monto_log_normalizado"] = scaler_log.fit_transform(np.log1p(df[["monto_compras"]])).ravel()
    parametros["log1p_monto_compras"] = {"min": float(scaler_log.data_min_[0]),
                                         "max": float(scaler_log.data_max_[0])}

    for columna in ["num_compras", "antiguedad_dias"]:
        sc = MinMaxScaler()
        df[f"{columna}_normalizado"] = sc.fit_transform(df[[columna]]).ravel()
        parametros[columna] = {"min": float(sc.data_min_[0]), "max": float(sc.data_max_[0])}
    return df, parametros


# ----------------------------------------------------------------------
# 4. Ingeniería de características
# ----------------------------------------------------------------------
def crear_caracteristicas_base(df):
    """Variables derivadas que se necesitan antes de normalizar."""
    df["antiguedad_dias"] = (FECHA_REFERENCIA - df["fecha_registro"]).dt.days
    df["ticket_promedio"] = (df["monto_compras"] / df["num_compras"]).round(2)
    return df


def crear_scores(df):
    """Scores derivados de las variables normalizadas."""
    # Código base
    df["score_promocional"] = (df["monto_normalizado"] * 100).round(2)
    # Extensión: score de fidelidad ponderado (valor, frecuencia y antigüedad)
    df["score_fidelidad"] = (100 * (0.5 * df["monto_log_normalizado"]
                                    + 0.3 * df["num_compras_normalizado"]
                                    + 0.2 * df["antiguedad_dias_normalizado"])).round(2)
    return df


def transformar(df):
    """Orquesta la fase de transformación y devuelve (procesado, rechazados, parámetros)."""
    validos, rechazados = limpiar(df)
    validos = crear_caracteristicas_base(validos)
    validos = discretizar(validos)
    validos, parametros = normalizar(validos)
    validos = crear_scores(validos)

    # Tipos compactos: categorías como 'category' (Parquet las guarda con diccionario)
    for columna in ["ciudad", "canal_preferido"]:
        validos[columna] = validos[columna].astype("category")
    validos["edad"] = validos["edad"].astype("Int16")
    validos["num_compras"] = validos["num_compras"].astype("int32")

    log.info("Transformación: %s filas x %s columnas", f"{len(validos):,}", validos.shape[1])
    return validos.reset_index(drop=True), rechazados.reset_index(drop=True), parametros
