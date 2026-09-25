cat << 'EOF' > README.md
# Benchmark de Ejecución en PySpark: Local vs. GCP Dataproc

Este repositorio contiene la guía técnica, el detalle de recursos de infraestructura y los scripts para ejecutar y evaluar el rendimiento del pipeline de procesamiento sobre el dataset de ofertas de trabajo de LinkedIn utilizando **PySpark**. 

---

## 🛠️ Recursos del Sistema y Tecnologías Utilizadas

### 1. Infraestructura Cloud (GCP Dataproc)
* **Clúster Distribuido:** 1 Nodo Máster + 2 Nodos Workers.
* **Tipo de Máquina por Nodo:** `n1-standard-2` (2 vCPUs, 8 GB RAM por nodo).
* **Almacenamiento del Clúster:** Discos de arranque de 50 GB por nodo.
* **Gestor de Recursos (Cluster Manager):** Apache YARN (configurado por defecto en Dataproc).
* **Almacenamiento de Datos:** Google Cloud Storage (GCS) en bucket distribuido.

### 2. Entorno de Software y Ejecución
* **Lenguaje:** Python 3.10+
* **Framework Distribuido:** PySpark (versión en Dataproc Image 2.1 - Debian 11)
* **Runtime de Java:** Java OpenJDK 11 / 17 (Requerido para JVM / Spark Core)
* **Formato de Datos:** Parquet con particionamiento distribuido (`gs://utec-linkedin-jobs-2026/processed_partitioned`)

---

## 💻 Configuración del Entorno Local (PyCharm)

Si deseas replicar las pruebas en tu máquina local:

### Prerrequisitos en la PC:
* **Java JDK 11 o 17** con la variable de entorno `JAVA_HOME` configurada.
* **Python 3.10+**.

### Instalación de Librerías (Terminal / PyCharm):
```bash
pip install pyspark pyarrow pandas findspark
