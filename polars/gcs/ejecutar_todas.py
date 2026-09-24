"""Envía las consultas Polars desde el master a un worker de Dask."""

import argparse
import csv
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

from dask.distributed import Client, get_worker


CARPETA = Path(__file__).resolve().parent
RESULTADOS = CARPETA / "resultados"
SCHEDULER = os.getenv("DASK_SCHEDULER_ADDRESS", "tcp://10.138.0.13:8786")


def ejecutar_en_worker(
    numero: int, codigo_comun: bytes, codigo_consulta: bytes
) -> tuple[str, str, dict[str, bytes]]:
    """Ejecuta Polars en el worker y devuelve solo sus archivos de salida."""
    nombre_worker = str(get_worker().name)
    with TemporaryDirectory(prefix="polars_gcs_") as temporal:
        carpeta = Path(temporal)
        (carpeta / "polars_comun.py").write_bytes(codigo_comun)
        script = carpeta / f"polars_consulta{numero}.py"
        script.write_bytes(codigo_consulta)

        proceso = subprocess.run(
            [sys.executable, str(script)],
            cwd=carpeta,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if proceso.returncode != 0:
            raise RuntimeError(
                f"La consulta {numero} falló en {nombre_worker} "
                f"(código {proceso.returncode}):\n{proceso.stdout}"
            )

        nombres = [f"consulta{numero}_tiempo.txt"]
        if numero != 1:  # La consulta 1 solo escribe su tiempo y muestra el resultado.
            nombres.append(f"consulta{numero}.csv")
        if numero == 3:
            nombres.append("consulta3_muestra.json")
        if numero == 4:
            nombres.append("consulta4_muestra.csv")
        archivos = {
            nombre: (carpeta / "resultados" / nombre).read_bytes()
            for nombre in nombres
        }
        return nombre_worker, proceso.stdout, archivos


def ejecutar_consulta(cliente: Client, direccion: str, numero: int) -> None:
    print(f"\nEnviando consulta {numero} a {direccion}", flush=True)
    futuro = cliente.submit(
        ejecutar_en_worker,
        numero,
        (CARPETA / "polars_comun.py").read_bytes(),
        (CARPETA / f"polars_consulta{numero}.py").read_bytes(),
        workers=[direccion],
        allow_other_workers=False,
        pure=False,
    )
    nombre_worker, salida, archivos = futuro.result()
    print(f"Consulta {numero} terminada en {nombre_worker}", flush=True)
    print(salida, end="" if salida.endswith("\n") else "\n")
    RESULTADOS.mkdir(exist_ok=True)
    for nombre, contenido in archivos.items():
        (RESULTADOS / nombre).write_bytes(contenido)


def reunir_tiempos() -> None:
    with (RESULTADOS / "tiempos_polars.csv").open(
        "w", newline="", encoding="utf-8"
    ) as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(["consulta", "segundos"])
        for numero in range(1, 11):
            segundos = (RESULTADOS / f"consulta{numero}_tiempo.txt").read_text(
                encoding="utf-8"
            ).strip()
            escritor.writerow([numero, segundos])
    print(f"\nTiempos guardados en {RESULTADOS / 'tiempos_polars.csv'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scheduler", default=SCHEDULER)
    parser.add_argument("--worker", default="w-0")
    parser.add_argument("--consulta", type=int, choices=range(1, 11))
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
                f"Workers disponibles: {nombres}. "
                f"Inícialo con --name {argumentos.worker}."
            )

        numeros = [argumentos.consulta] if argumentos.consulta else range(1, 11)
        for numero in numeros:
            ejecutar_consulta(cliente, direcciones[0], numero)

    if argumentos.consulta is None:
        reunir_tiempos()


if __name__ == "__main__":
    main()
