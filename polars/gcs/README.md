# Consultas Polars desde Google Cloud Storage

Las consultas leen los CSV de `gs://utec-linkedin-jobs-2026/raw/`. La consulta
7 lee las partes Parquet de `job_summary` preparadas por Dask, en vez de leer
directamente el CSV multilínea de 4.8 GB.

## Ejecutar desde el master en `w-0`

El master se conecta al scheduler Dask y envía cada consulta exclusivamente al
worker llamado `w-0`. El código de `polars_comun.py` y de la consulta se envía
con la tarea; **no hace falta copiar `polars_proyecto/` al worker**. Allí se
ejecuta Polars en un proceso Python, que lee los datos directamente de GCS. Los
archivos de resultado regresan al master y se guardan en
`polars_proyecto/gcs/resultados/`. Las consultas se ejecutan una por una.
Esta ejecución no distribuye una consulta entre varios workers.

1. Copia la carpeta `gcs/` completa al master. Desde esta máquina local:

   ```bash
   gcloud compute ssh cluster-proyecto-m --zone=us-west1-c \
     --command='mkdir -p ~/polars_proyecto'
   gcloud compute scp --recurse \
     "/Users/davidsilva/UTEC/2026-2/Big Data/S2/notebook/polars_proyecto/gcs" \
     cluster-proyecto-m:~/polars_proyecto/ \
     --zone=us-west1-c
   ```

2. Instala `requirements.txt` en el master. En `w-0`, instala Polars,
   `google-auth` y `gcsfs` con el mismo Python que utiliza el proceso
   `dask worker`.
   `w-0` necesita credenciales con permiso de lectura en el bucket. Puede usar
   la cuenta de servicio de la VM; si hay una sesión activa de `gcloud`, el
   código usa su token. Si no, usa las credenciales de aplicación de
   `google-auth`.

   ```bash
   python -m pip install -r ~/polars_proyecto/gcs/requirements.txt
   ```

   En `w-0`, donde no se copia la carpeta, instala directamente:

   ```bash
   python -m pip install 'polars>=1.44,<2' 'google-auth>=2,<3' 'gcsfs>=2026.8,<2027'
   ```

3. Inicia el scheduler en el master y el worker con el nombre `w-0` (si ya
   están activos para Dask, reutilízalos):

   ```bash
   # En el master
   dask scheduler --host IP_DEL_SCHEDULER

   # En w-0
   dask worker tcp://IP_DEL_SCHEDULER:8786 --nworkers 1 --nthreads 2 --name w-0
   ```

4. Antes de la consulta 7, comprueba que exista
   `gs://utec-linkedin-jobs-2026/processed/job_summary_parquet/_SUCCESS`. Si
   ya preparaste Dask para la consulta 7, reutiliza esos mismos Parquet. Si
   aún no existen, sigue la preparación de
   [`dask_proyecto/README.md`](../../dask_proyecto/README.md) y ejecuta
   `python dask_proyecto/preparar_job_summary.py` desde el master una sola vez.
   La preparación requiere permiso de escritura en `processed/` para `w-0`.

5. En el master, ejecuta:

   ```bash
   python ~/polars_proyecto/gcs/ejecutar_todas.py
   ```

   Para otra dirección del scheduler, usa `--scheduler` o la variable
   `DASK_SCHEDULER_ADDRESS`. Para ejecutar solo la consulta 7:

   ```bash
   python ~/polars_proyecto/gcs/ejecutar_todas.py --consulta 7
   ```

   Si el Parquet preparado está en otra ruta, pasa
   `--resumen-parquet gs://BUCKET/RUTA` o configura
   `POLARS_JOB_SUMMARY_PARQUET` en el master. El ejecutor transmite esa ruta al
   proceso Polars de `w-0`.

El ejecutor reúne los diez tiempos en `tiempos_polars.csv` al completar todas
las consultas. Los tiempos individuales incluyen lectura de GCS y cálculo;
excluyen el envío de la tarea, la transferencia de resultados y su escritura
en el master. La consulta 1 solo guarda su tiempo y muestra los conteos en
pantalla; las consultas 2 a 10 guardan también su CSV.

## Ejecutar directamente en una máquina

Si ejecutas `polars_consulta7.py` directamente, toda la consulta se ejecuta en
esa máquina, sin pasar por el scheduler:

```bash
python polars_proyecto/gcs/polars_consulta7.py
```
