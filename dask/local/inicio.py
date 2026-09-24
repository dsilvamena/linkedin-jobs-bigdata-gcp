"""Configuración de Dask para ejecutar consultas en esta Mac."""

from pathlib import Path
import sys

from dask.distributed import Client


PROYECTO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROYECTO))

from dask_consultas import Datos, ejecutar_consulta, reunir_tiempos  # noqa: E402


RESULTADOS = Path(__file__).resolve().parent / "resultados"
DATOS = Datos(str(PROYECTO.parent / "data_proyecto"))


def cliente() -> Client:
    # Misma topología prevista para GCS: dos workers con dos hilos cada uno.
    return Client(
        n_workers=2,
        threads_per_worker=2,
        processes=True,
        memory_limit="8GiB",
        dashboard_address=None,
    )


def ejecutar_individual(numero: int) -> None:
    with cliente() as conexion:
        print("Workers locales:", len(conexion.scheduler_info()["workers"]))
        ejecutar_consulta(numero, DATOS, RESULTADOS)
