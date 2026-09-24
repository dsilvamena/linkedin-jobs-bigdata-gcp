"""Convierte el CSV multilínea de descripciones a Parquet particionado.

Se lanza desde el master una sola vez, antes de las consultas del clúster.
El worker elegido lee el CSV por bloques de filas para respetar los saltos de
línea entre comillas y escribe cada bloque como un Parquet independiente.
"""

import argparse
import json
import os

import fsspec
import pandas as pd
from dask.distributed import Client, get_worker


ORIGEN_GCS = "gs://utec-linkedin-jobs-2026/raw/job_summary.csv"
DESTINO_GCS = "gs://utec-linkedin-jobs-2026/processed/job_summary_parquet"
SCHEDULER = os.getenv("DASK_SCHEDULER_ADDRESS", "tcp://10.138.0.13:8786")


def convertir(
    origen: str,
    destino: str,
    filas_por_parte: int = 20_000,
    token_gcs: str = "cloud",
) -> tuple[int, int]:
    if filas_por_parte < 1:
        raise ValueError("--filas-por-parte debe ser mayor que cero")

    opciones_origen = {"token": token_gcs} if origen.startswith("gs://") else None
    opciones_destino = {"token": token_gcs} if destino.startswith("gs://") else {}
    sistema, carpeta = fsspec.core.url_to_fs(destino, **opciones_destino)
    carpeta = carpeta.rstrip("/")
    if sistema.exists(carpeta):
        raise FileExistsError(
            f"El destino ya existe: {destino}. Elige otro para no sobrescribir datos."
        )

    partes = 0
    filas = 0
    with pd.read_csv(
        origen,
        usecols=["job_link", "job_summary"],
        dtype=str,
        keep_default_na=False,
        chunksize=filas_por_parte,
        storage_options=opciones_origen,
    ) as bloques:
        sistema.makedirs(carpeta, exist_ok=False)
        for partes, bloque in enumerate(bloques, start=1):
            ruta_parte = f"{carpeta}/parte-{partes:05d}.parquet"
            with sistema.open(ruta_parte, "wb") as archivo:
                bloque.to_parquet(archivo, engine="pyarrow", index=False)
            filas += len(bloque)
            print(f"Parte {partes}: {len(bloque)} filas", flush=True)

    if partes == 0:
        raise ValueError("El CSV no contiene filas de datos")

    # La consulta 7 solo acepta un directorio que terminó de convertirse.
    with sistema.open(f"{carpeta}/_SUCCESS", "wt") as archivo:
        json.dump({"origen": origen, "partes": partes, "filas": filas}, archivo)
    return partes, filas


def convertir_en_worker(
    origen: str, destino: str, filas_por_parte: int, token_gcs: str
) -> tuple[str, int, int]:
    nombre = get_worker().name
    partes, filas = convertir(origen, destino, filas_por_parte, token_gcs)
    return str(nombre), partes, filas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origen", default=ORIGEN_GCS)
    parser.add_argument("--destino", default=DESTINO_GCS)
    parser.add_argument("--filas-por-parte", type=int, default=20_000)
    parser.add_argument("--token-gcs", default="cloud")
    parser.add_argument("--scheduler", default=SCHEDULER)
    parser.add_argument("--worker", default="w-0", help="Nombre del worker que convierte")
    argumentos = parser.parse_args()

    with Client(argumentos.scheduler, timeout="30s") as cliente:
        workers = cliente.scheduler_info()["workers"]
        direcciones = [
            direccion for direccion, informacion in workers.items()
            if informacion.get("name") == argumentos.worker
        ]
        if len(direcciones) != 1:
            nombres = [informacion.get("name") for informacion in workers.values()]
            raise RuntimeError(
                f"No se encontró un único worker '{argumentos.worker}'. "
                f"Workers disponibles: {nombres}. Inícialo con --name {argumentos.worker}."
            )
        print(f"Enviando conversión a {argumentos.worker} ({direcciones[0]})", flush=True)
        futuro = cliente.submit(
            convertir_en_worker,
            argumentos.origen,
            argumentos.destino,
            argumentos.filas_por_parte,
            argumentos.token_gcs,
            workers=direcciones,
            allow_other_workers=False,
            pure=False,
        )
        nombre, partes, filas = futuro.result()
    print(f"Conversión completa en {nombre}: {filas} filas en {partes} archivos Parquet")
    print(f"Destino: {argumentos.destino}")


if __name__ == "__main__":
    main()
