"""Configuración para enviar las consultas del master al clúster Dask."""

import os
from pathlib import Path
import sys

from dask.distributed import Client


PROYECTO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROYECTO))

from dask_consultas import Datos, ejecutar_consulta, reunir_tiempos  # noqa: E402


RESULTADOS = Path(__file__).resolve().parent / "resultados"
DATOS = Datos(
    "gs://utec-linkedin-jobs-2026/raw",
    storage_options={"token": "cloud"},
    resumen_parquet=os.getenv(
        "DASK_JOB_SUMMARY_PARQUET",
        "gs://utec-linkedin-jobs-2026/processed/job_summary_parquet",
    ),
)
DIRECCION_SCHEDULER = os.getenv(
    "DASK_SCHEDULER_ADDRESS", "tcp://10.138.0.13:8786"
)


def cliente() -> Client:
    conexion = Client(DIRECCION_SCHEDULER, timeout="30s")
    try:
        conexion.wait_for_workers(2, timeout=60)
    except Exception:
        conexion.close()
        raise
    workers = conexion.scheduler_info()["workers"]
    if len(workers) != 2 or any(info["nthreads"] != 2 for info in workers.values()):
        conexion.close()
        raise RuntimeError(
            "Se requieren exactamente dos workers con dos hilos cada uno; "
            f"configuración detectada: {[info['nthreads'] for info in workers.values()]}"
        )
    print("Scheduler:", DIRECCION_SCHEDULER)
    print("Workers disponibles:", len(workers))
    for direccion, informacion in workers.items():
        print(
            " -", direccion,
            "host:", informacion.get("host", "desconocido"),
            "hilos:", informacion["nthreads"],
        )
    return conexion


def ejecutar_individual(numero: int) -> None:
    with cliente():
        ejecutar_consulta(numero, DATOS, RESULTADOS)
