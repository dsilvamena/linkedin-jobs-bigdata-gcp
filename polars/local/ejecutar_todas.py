"""Ejecuta las diez consultas de Polars con los CSV locales."""

import csv
from pathlib import Path
import subprocess
import sys


CARPETA = Path(__file__).resolve().parent
RESULTADOS = CARPETA / "resultados"


def main() -> None:
    for numero in range(1, 11):
        print(f"\nEjecutando consulta {numero} localmente", flush=True)
        subprocess.run(
            [sys.executable, str(CARPETA / f"polars_consulta{numero}.py")],
            check=True,
        )

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


if __name__ == "__main__":
    main()
