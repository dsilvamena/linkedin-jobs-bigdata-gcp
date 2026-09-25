# Consultas Modin

Este directorio contiene las 10 consultas del proyecto implementadas con
**Modin** y **Ray** sobre el dataset **1.3M LinkedIn Jobs & Skills (2024)**.

Los datos se leen principalmente desde Parquet en Google Cloud Storage:

```text
gs://utec-linkedin-jobs-2026/processed_partitioned/postings_parquet
gs://utec-linkedin-jobs-2026/processed_partitioned/skills_parquet
gs://utec-linkedin-jobs-2026/processed/job_summary_parquet
```

## Estructura

```text
modin/
├── modin_comun.py
├── modin_consulta1.py
├── ...
├── modin_consulta10.py
└── resultados/
```

`modin_comun.py` contiene las rutas de GCS, la configuración de Ray y funciones
compartidas.

## Consultas

| # | Consulta |
|---|---|
| 1 | Eliminar duplicados por `job_link` |
| 2 | Conteo y tratamiento de nulos |
| 3 | Transformar `job_skills` a listas |
| 4 | Filtrar ofertas remotas |
| 5 | Top 10 empresas |
| 6 | Top países |
| 7 | Promedio de longitud de descripción por `job_level` |
| 8 | Top 20 habilidades |
| 9 | Ranking completo de empresas |
| 10 | Promedio de habilidades por oferta |

## Configuración

Modin utiliza Ray como backend:

```python
os.environ["MODIN_ENGINE"] = "ray"
os.environ["MODIN_NPARTITIONS"] = "16"
```

La conexión se realiza con:

```python
ray.init(address="auto")
```

La ejecución se realizó en Dataproc con un master como **Ray Head** y dos
workers.

## Iniciar Ray

Master:

```bash
ray start --head \
  --node-ip-address=10.138.0.13 \
  --port=6379 \
  --num-cpus=0
```

Worker 0:

```bash
ray start \
  --address=10.138.0.13:6379 \
  --node-ip-address=10.138.0.12 \
  --num-cpus=1
```

Worker 1:

```bash
ray start \
  --address=10.138.0.13:6379 \
  --node-ip-address=10.138.0.14 \
  --num-cpus=1
```

Para verificar:

```bash
ray status
```

## Dependencias

```text
Python 3.12
Modin 0.37.1
Ray 2.58.0
Pandas 2.3.2
PyArrow 20.0.0
```

## Ejecución

Desde el master:

```bash
cd /home/Jose/linkedin-jobs-bigdata-gcp/modin
```

Ejemplo:

```bash
python3 -u modin_consulta1.py
python3 -u modin_consulta8.py
python3 -u modin_consulta10.py
```

El resto se ejecuta de la misma forma con `modin_consultaN.py`.

Los resultados se guardan en:

```text
modin/resultados/
```

Cada consulta genera:

```text
consultaN.csv
consultaN_tiempo.txt
```

## Manejo de memoria

Las consultas simples utilizan Modin directamente.

Las consultas 7, 8 y 10 requieren procesamiento adicional por el tamaño de las
columnas de texto. Para estas consultas se utiliza **PyArrow** por lotes y
**SQLite** como almacenamiento temporal para reducir el uso de RAM.

## Tiempos

| Consulta | Tiempo (s) |
|---|---:|
| 1 | 6.70 |
| 2 | 78.18 |
| 3 | 55.41 |
| 4 | 8.97 |
| 5 | 6.21 |
| 6 | 4.36 |
| 7 | 85.13 |
| 8 | 93.52 |
| 9 | 9.32 |
| 10 | 41.13 |

Estos resultados permiten comparar Modin con **Polars, Dask y PySpark** usando
las mismas consultas y el mismo conjunto de datos.