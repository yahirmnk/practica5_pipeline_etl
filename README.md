# Práctica 5: Pipeline ETL Automatizado a AWS S3 / PostgreSQL

Pipeline ETL modular en Python que **extrae** clientes de un sistema
operacional en **PostgreSQL**, los **transforma** (limpieza, discretización
con `pd.cut`/`pd.qcut`, normalización `MinMaxScaler` e ingeniería de
características) y los **carga** en formato **Apache Parquet** serializado en
memoria (`io.BytesIO`) en un Data Lake en **AWS S3** con zonas
`raw → processed` (más `quarantine`), y en una tabla analítica de PostgreSQL.

## Estructura

```
practica5/
├── etl/
│   ├── config.py          # lectura de config.yaml y conexiones
│   ├── extraccion.py      # Extract: consulta SQL o CSV de la zona raw
│   ├── transformacion.py  # Transform: limpieza, binning, MinMax, features
│   └── carga.py           # Load: CSV/Parquet en memoria, S3, PostgreSQL
├── preparar_fuente.py     # crea la tabla fuente operaciones.clientes
├── ejecutar_pipeline.py   # orquestador del pipeline (punto de entrada)
├── comparar_formatos.py   # CSV vs Parquet: tamaño y tiempos
├── ejecutar_pipeline.bat  # para el Programador de tareas de Windows
├── config.example.yaml    # plantilla de configuración (sin contraseña)
├── requirements.txt
└── salidas/               # resultados de la comparación de formatos
```

## Configuración

```bash
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy config.example.yaml config.yaml     # escribe tu contraseña y tu bucket
```

- **PostgreSQL:** usuario y contraseña en `config.yaml` (excluido de Git).
- **AWS:** credenciales temporales de AWS Academy en `~/.aws/credentials`
  (nunca dentro del proyecto).

## Ejecución

```bash
python preparar_fuente.py                 # 1 vez: crea 20,000 clientes en PostgreSQL
python ejecutar_pipeline.py               # PostgreSQL -> S3 (raw/processed) + PostgreSQL
python ejecutar_pipeline.py --fuente s3   # reprocesa el CSV de la zona raw
python ejecutar_pipeline.py --local       # sin AWS: zonas en la carpeta data_lake/
python comparar_formatos.py               # CSV vs Parquet
```

## Zonas del Data Lake (S3)

| Zona | Objeto | Formato |
|---|---|---|
| raw | `raw/clientes/fecha_carga=AAAA-MM-DD/clientes.csv` | CSV (copia fiel del origen) |
| processed | `processed/clientes_features/fecha_carga=AAAA-MM-DD/clientes_features.parquet` | Parquet + snappy |
| processed | `.../_metadata.json` | parámetros de MinMax, bins y conteos |
| quarantine | `quarantine/clientes/fecha_carga=AAAA-MM-DD/rechazados.csv` | registros inválidos |

## Transformaciones

| Columna | Técnica |
|---|---|
| `categoria_edad` | `pd.cut` con bins `[0, 25, 50, 100]` → Joven / Adulto / Senior (+ Desconocido) |
| `segmento_monto` | `pd.qcut` en cuartiles → Bajo / Medio / Alto / Premium |
| `monto_normalizado` | `MinMaxScaler` sobre `monto_compras` |
| `monto_log_normalizado` | `MinMaxScaler` sobre `log(1 + monto)` (corrige el sesgo) |
| `num_compras_normalizado`, `antiguedad_dias_normalizado` | `MinMaxScaler` |
| `score_promocional` | `monto_normalizado × 100` (código base) |
| `ticket_promedio`, `antiguedad_dias`, `score_fidelidad` | ingeniería de características |

## Resultados (CSV vs Parquet, 198,400 filas)

| Formato | Tamaño | Lectura completa | Lectura de 2 columnas |
|---|---|---|---|
| CSV | 34.4 MB | 435 ms | 224 ms |
| Parquet snappy | 10.3 MB (30 %) | 24 ms | 4 ms |
| Parquet zstd | 8.4 MB (24 %) | 30 ms | 5 ms |
