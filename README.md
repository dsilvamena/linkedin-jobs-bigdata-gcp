# linkedin-jobs-bigdata-gcp

Proyecto grupal del curso **Big Data** (UTEC) enfocado en el procesamiento masivo y análisis distribuido de más de 1.3 millones de ofertas laborales y habilidades extraídas de LinkedIn (Dataset: *1.3M LinkedIn Jobs & Skills, 2024*).

El almacenamiento central del Data Lake se encuentra organizado en Google Cloud Storage bajo el bucket `gs://utec-linkedin-jobs-2026/`, dividiendo las capas en `raw/` (archivos fuente CSV), `processed/` (datos transformados y particionados en Parquet) y `output/` (salidas analíticas).

---

## 🚀 Fase 5: Procesamiento Distribuido (Polars y Dask)

En la **Fase 5** del proyecto se implementaron, optimizaron y evaluaron **10 consultas analíticas masivas** sobre los datos, ejecutadas bajo dos paradigmas de procesamiento de alto rendimiento:

1. **Polars**: Procesamiento columnar vectorizado en memoria optimizado con Rust, aprovechando multihilo nativo y evaluación perezosa (*LazyFrames*).
2. **Dask**: Procesamiento distribuido basado en grafos de tareas dirigidos acíclicos (DAGs) y particionado de datos (*Dask DataFrame* y `dask.distributed`), tanto en ejecución local como en clúster multiesclavo.

---

### 📋 Las 10 Consultas Analíticas Implementadas

Ambos frameworks cubren de manera idéntica las siguientes consultas conceptuales, garantizando consistencia analítica y comparabilidad directa de tiempos de cómputo:

| # | Consulta | Descripción Técnica |
|---|----------|---------------------|
| **1** | **Eliminar duplicados por `job_link`** | Deduplicación de ofertas manteniendo registros únicos según el identificador de la vacante. |
| **2** | **Conteo y tratamiento de valores nulos** | Análisis de completitud por columna, cuantificación de nulos e imputación/filtrado correspondiente. |
| **3** | **Transformación de `job_skills` a listas** | Parseo del campo de texto de habilidades separadas por comas a tipos array/lista estructurada (`split(', ')`). |
| **4** | **Filtrado de ofertas remotas** | Segmentación de vacantes con modalidad estrictamente remota (`job_type == 'Remote'`). |
| **5** | **Top 10 empresas con más publicaciones** | Agrupación por empresa (`company`), conteo de publicaciones y ranking descendente de los 10 principales empleadores. |
| **6** | **Top 10 países con más ofertas** | Agrupación geográfica por país/ubicación y determinación del top 10 con mayor demanda laboral. |
| **7** | **Longitud promedio de descripción por `job_level`** | Cálculo de longitud de texto sobre `job_summary.csv` (~4.8 GB) agrupado por nivel de antigüedad (`job_level`). En Dask incluye preprocesamiento a formato Apache Parquet particionado. |
| **8** | **Habilidades más demandadas** | Desanidado (*explode*) de las listas de habilidades por oferta y agregación para identificar las skills con mayor frecuencia relativa. |
| **9** | **Ranking global de empresas por ofertas** | Ordenamiento descendente completo de la masa empresarial según volumen total de vacantes publicadas. |
| **10** | **Cruce relacional y promedio de skills** | *Join* relacional entre `job_skills` y `linkedin_job_postings` mediante `job_link`, calculando el promedio de competencias requeridas por oferta publicada. |

---

## 📁 Estructura del Repositorio

```text
.
├── README.md                     # Documentación principal de la Fase 5
├── .gitignore                    # Reglas de exclusión de datos voluminosos y cachés
├── tiempos_dask.csv              # Tiempos reales de ejecución en Clúster Dask sobre GCS (descargado de GCP)
├── tiempos_polars.csv            # Tiempos reales de ejecución con Polars sobre GCS (descargado de GCP)
├── dask/                         # Implementación con Dask
│   ├── README.md                 # Guía detallada de ejecución Dask
│   ├── dask_consultas.py         # Lógica central modular de las 10 consultas
│   ├── preparar_job_summary.py   # Script de conversión por bloques a Parquet en GCS
│   ├── prueba_dask.py            # Test de conectividad con Dask Scheduler
│   ├── local/                    # Ejecución local con workers e hilos locales
│   │   ├── inicio.py
│   │   ├── ejecutar_todas.py     # Runner batch local (genera tiempos_dask.csv)
│   │   ├── dask_consulta[1-10].py# Scripts individuales de consulta
│   │   └── resultados/           # Archivos generados y métricas locales
│   └── cluster/                  # Ejecución en clúster GCP conectada al Scheduler
│       ├── inicio.py
│       ├── ejecutar_todas.py     # Runner batch en clúster
│       └── dask_consulta[1-10].py# Scripts individuales para clúster
└── polars/                       # Implementación con Polars
    ├── README.md                 # Guía general de ejecución Polars
    ├── local/                    # Ejecución sobre CSVs locales
    │   ├── polars_comun.py       # Funciones compartidas de lectura y utilidades
    │   ├── ejecutar_todas.py     # Runner batch local (genera tiempos_polars.csv)
    │   ├── polars_consulta[1-10].py
    │   └── resultados/           # Salidas CSV y tiempos locales
    └── gcs/                      # Ejecución leyendo directamente desde Google Cloud Storage
        ├── README.md             # Guía de orquestación en worker w-0
        ├── requirements.txt      # Dependencias para entorno GCP
        ├── polars_comun.py       # Lectura de GCS con autenticación y streaming
        ├── ejecutar_todas.py     # Runner por lotes en worker de clúster
        └── polars_consulta[1-10].py
```

> **Nota sobre el directorio de datos (`data/`)**: Por restricciones de tamaño de GitHub (límite de 100 MB por archivo) y de acuerdo a las directivas del proyecto, los archivos fuente originales (`job_summary.csv` 4.8 GB, `job_skills.csv` 642 MB y `linkedin_job_postings.csv` 396 MB) no se versionan en Git y deben residir localmente en una carpeta `data/` o ser leídos directamente desde el Data Lake en GCP: `gs://utec-linkedin-jobs-2026/raw/`.

---

## ⚙️ Guía de Ejecución

### 1. Entorno y Dependencias

Se requiere Python 3.10 o superior. Puedes sincronizar las dependencias con `uv` o instalarlas vía `pip`:

```bash
pip install polars dask[dataframe,distributed] pandas pyarrow gcsfs google-auth
```

### 2. Ejecutar Polars

- **En local**:
  ```bash
  python polars/local/ejecutar_todas.py
  # O una consulta específica:
  python polars/local/polars_consulta7.py
  ```

- **Desde Google Cloud Storage (nodo worker `w-0`)**:
  ```bash
  python polars/gcs/ejecutar_todas.py
  ```

### 3. Ejecutar Dask

- **En local (2 workers / 2 hilos)**:
  ```bash
  python dask/local/ejecutar_todas.py
  # O una consulta específica:
  python dask/local/dask_consulta7.py
  ```

- **En Clúster Distribuido (Scheduler & Workers en GCP)**:
  ```bash
  # Iniciar scheduler (en master):
  dask scheduler --host IP_DEL_SCHEDULER

  # Iniciar workers (en máquinas worker):
  dask worker tcp://IP_DEL_SCHEDULER:8786 --nworkers 1 --nthreads 2 --name w-0
  dask worker tcp://IP_DEL_SCHEDULER:8786 --nworkers 1 --nthreads 2 --name w-1

  # Preparación previa de Parquet para consulta 7 (en w-0):
  python dask/preparar_job_summary.py

  # Ejecución de todas las consultas:
  DASK_SCHEDULER_ADDRESS=tcp://IP_DEL_SCHEDULER:8786 python dask/cluster/ejecutar_todas.py
  ```

---

## ⏱️ Comparativa y Medición de Rendimiento

Cada consulta mide de manera aislada el tiempo exacto de lectura de datos y cómputo analítico (`time.time()`), excluyendo la sobrecarga de escritura en disco.

Los archivos ubicados en la raíz del repositorio corresponden a los **resultados reales descargados directamente del entorno de Google Cloud Platform (GCS / Clúster)**:

- **[`tiempos_dask.csv`](tiempos_dask.csv)**: Tiempos de ejecución obtenidos en el **clúster distribuido de Dask** en GCP (conectado al Scheduler en el master y ejecutando sobre los workers remotos `w-0` y `w-1`), leyendo los datos directamente desde Google Cloud Storage (`gs://utec-linkedin-jobs-2026/raw/` y la capa Parquet preparada en GCS).
- **[`tiempos_polars.csv`](tiempos_polars.csv)**: Tiempos de ejecución obtenidos con **Polars** ejecutado en el nodo worker de GCP (`w-0`), leyendo directamente de Google Cloud Storage con autenticación y motor streaming.

Adicionalmente, en `dask/local/resultados/tiempos_dask.csv` y `polars/local/resultados/tiempos_polars.csv` se conservan los tiempos de referencia generados en ejecuciones locales sobre la máquina de desarrollo.

### 📊 Comparativa de Tiempos Reales en GCP (GCS)

| # | Consulta | Dask Clúster GCS (s) | Polars GCS (s) |
|---|----------|----------------------|----------------|
| **1** | Deduplicar por `job_link` | 13.49 s | **2.79 s** |
| **2** | Conteo y tratamiento de nulos | 5.48 s | **3.32 s** |
| **3** | Transformar `job_skills` a lista | 11.50 s | **2.54 s** |
| **4** | Filtrar ofertas remotas | 6.57 s | **1.31 s** |
| **5** | Top 10 empresas con más publicaciones | 3.00 s | **1.32 s** |
| **6** | Top 10 países con más ofertas | 2.76 s | **1.42 s** |
| **7** | Longitud promedio de descripción (`job_summary`) | **16.85 s** | 21.34 s |
| **8** | Habilidades más demandadas (`explode`) | 14.02 s | **6.64 s** |
| **9** | Ranking global de empresas por ofertas | 3.13 s | **1.50 s** |
| **10** | Cruce relacional y promedio de skills | 13.30 s | **4.22 s** |
| **Total** | **Tiempo acumulado** | **~90.10 s** | **~46.57 s** |

