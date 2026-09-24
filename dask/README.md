# Consultas Dask

El mismo conjunto de diez consultas se ejecuta de dos formas:

- `local/`: dos procesos worker en esta Mac, cada uno con dos hilos. Dask reparte
  las particiones de los CSV entre ambos procesos, aunque haya una sola máquina.
- `cluster/`: el script se ejecuta en el master y se conecta a un scheduler Dask.
  El scheduler envía las tareas a los workers remotos. `prueba_dask.py` es una
  prueba de conexión al scheduler, no inicia el scheduler.

## Ejecutar localmente

Desde la raíz de este proyecto:

```bash
uv sync
uv run python dask_proyecto/local/ejecutar_todas.py
```

Para una sola consulta, por ejemplo la 7:

```bash
uv run python dask_proyecto/local/dask_consulta7.py
```

Los resultados y tiempos se guardan en `local/resultados/`.

## Ejecutar desde el master

Copia todo `dask_proyecto/` al master. Instala en master y en los dos workers
versiones compatibles de `dask[dataframe,distributed]`, `pandas`, `pyarrow` y
`gcsfs`. Los workers necesitan permiso de lectura sobre
`gs://utec-linkedin-jobs-2026/raw/` y el Parquet preparado. El master necesita
permiso de lectura sobre el Parquet para comprobar que la conversión terminó.
Además, `w-0` necesita permiso de escritura en `processed/` durante la
conversión.

Inicia el scheduler en el master y `w-0` en la primera máquina worker. Ese
worker tiene dos hilos, aunque la lectura secuencial del CSV ocupa una sola
tarea de Dask:

```bash
# En el master
dask scheduler --host IP_DEL_SCHEDULER

# En la primera máquina worker
dask worker tcp://IP_DEL_SCHEDULER:8786 --nworkers 1 --nthreads 2 --name w-0
```

Desde el master, envía la conversión una sola vez a `w-0`. El master solo envía
la tarea y espera el resultado; `w-0` lee el CSV, crea las partes y las sube a
GCS. `w-1` no necesita estar conectado durante esta preparación:

```bash
python dask_proyecto/preparar_job_summary.py
```

Cuando termine, inicia `w-1` en la segunda máquina worker para ejecutar las
consultas con los dos workers:

```bash
dask worker tcp://IP_DEL_SCHEDULER:8786 --nworkers 1 --nthreads 2 --name w-1
```

El script escribe archivos `parte-*.parquet` y al final un marcador `_SUCCESS`
en `gs://utec-linkedin-jobs-2026/processed/job_summary_parquet/`. Si el destino
ya existe, se detiene sin sobrescribirlo. Si una conversión se interrumpe,
revisa el directorio incompleto y usa otro destino para repetirla. Puedes
cambiar las rutas con `--origen` y `--destino`, y el worker con `--worker`.
Si cambias el destino, configura `DASK_JOB_SUMMARY_PARQUET` con la misma ruta
al ejecutar el clúster. La conversión se mide por separado del tiempo de la
consulta 7.

El cliente de las consultas comprueba que estén conectados exactamente dos
workers con dos hilos cada uno antes de iniciar. Ajusta la memoria disponible
por worker según la máquina donde se ejecute.

Con la dirección predeterminada del scheduler (`tcp://10.138.0.13:8786`):

```bash
python dask_proyecto/cluster/ejecutar_todas.py
```

Para otra dirección de scheduler:

```bash
DASK_SCHEDULER_ADDRESS=tcp://IP_DEL_SCHEDULER:8786 python dask_proyecto/cluster/ejecutar_todas.py
```

También se puede ejecutar `cluster/dask_consulta7.py` por separado. Los
resultados y tiempos se guardan en `cluster/resultados/` del master. El cliente
espera dos workers antes de comenzar.

`job_summary.csv` contiene saltos de línea dentro de campos entrecomillados.
El preparador usa pandas en `w-0` por bloques para preservar cada registro y
escribir Parquet particionado. En el clúster, la consulta 7 lee ese Parquet con
`dd.read_parquet`; los workers calculan las longitudes, cruzan y agrupan.
Localmente, la consulta 7 sigue leyendo el CSV por bloques con pandas y envía
las longitudes a los workers locales, sin requerir la preparación previa.

## Relación con los ejemplos de clase

Las consultas siguen el patrón de `Actividad_05.1_Dask_Cluster.ipynb` y
`Actividad_05.2_Dask_DataFrame.ipynb`: crear un `Client`, leer con
`dd.read_csv` en particiones, encadenar filtros y transformaciones, agrupar o
cruzar datos y llamar a `.compute()` al obtener el resultado final. Las
funciones numeradas están en `dask_consultas.py` para reutilizar la misma
lógica en local y en GCS; los archivos `dask_consultaN.py` solo configuran la
ejecución. La preparación del CSV multilínea para la consulta 7 es el paso
adicional explicado arriba.

Cada consulta guarda `consultaN.csv` y `consultaN_tiempo.txt`. Al ejecutar las
diez juntas se crea `tiempos_dask.csv`. Los tiempos incluyen la lectura y el
cálculo, pero excluyen la escritura de resultados. El runner del clúster también
crea `cluster/resultados/tiempos_consultas.txt` con el tiempo mostrado al
terminar cada una de las diez consultas.
