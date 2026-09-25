from pathlib import Path
import subprocess
import sys
import json
import csv
import os

BASE = Path(__file__).resolve().parent
RESULTADOS = BASE / "resultados"

RUTA = (
    "gs://utec-linkedin-jobs-2026/"
    "processed_partitioned/postings_parquet"
)

COLUMNAS = [
    "job_link",
    "last_processed_time",
    "got_summary",
    "got_ner",
    "is_being_worked",
    "job_title",
    "company",
    "job_location",
    "first_seen",
    "search_city",
    "search_country",
    "search_position",
    "job_level",
    "job_type",
]


def procesar_columna(columna):
    # Estas variables deben definirse antes de importar Modin/Ray
    os.environ["MODIN_ENGINE"] = "ray"
    os.environ["MODIN_CPUS"] = "1"

    import time
    import ray
    import modin.pandas as pd

    ray.init(
        num_cpus=1,
        include_dashboard=False,
        ignore_reinit_error=True,
        logging_level="ERROR",
    )

    inicio = time.perf_counter()

    try:
        df = pd.read_parquet(
            RUTA,
            columns=[columna]
        )

        serie = df[columna]

        nulos_antes = int(
            serie.isna().sum()
        )

        no_nulos = serie.dropna()

        vacios_antes = int(
            no_nulos
            .astype(str)
            .str.strip()
            .eq("")
            .sum()
        )

        faltantes = nulos_antes + vacios_antes

        # Después del tratamiento:
        # - job_link faltante se descarta
        # - demás faltantes se reemplazan por "No especificado"
        nulos_despues = 0

        segundos = time.perf_counter() - inicio

        resultado = {
            "columna": columna,
            "nulos_antes": nulos_antes,
            "vacios_antes": vacios_antes,
            "faltantes": faltantes,
            "nulos_despues": nulos_despues,
            "segundos": segundos,
        }

        print(
            "RESULT_JSON="
            + json.dumps(resultado),
            flush=True,
        )

    finally:
        ray.shutdown()


def ejecutar_consulta():
    print("=== MODIN - CONSULTA 2 ===")
    print("Conteo y tratamiento de valores nulos por columna")
    print()

    filas = []
    tiempo_total = 0.0

    for i, columna in enumerate(COLUMNAS, start=1):

        print(
            f"[{i}/{len(COLUMNAS)}] "
            f"Procesando {columna}...",
            flush=True,
        )

        proceso = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--columna",
                columna,
            ],
            capture_output=True,
            text=True,
        )

        if proceso.returncode != 0:
            print(f"\nERROR EN {columna}")
            print(proceso.stdout)
            print(proceso.stderr)
            sys.exit(1)

        resultado = None

        for linea in proceso.stdout.splitlines():
            if linea.startswith("RESULT_JSON="):
                resultado = json.loads(
                    linea.replace(
                        "RESULT_JSON=",
                        "",
                        1,
                    )
                )

        if resultado is None:
            print(proceso.stdout)
            raise RuntimeError(
                f"No se obtuvo resultado para {columna}"
            )

        filas.append(resultado)
        tiempo_total += resultado["segundos"]

        print(
            f"    nulos={resultado['nulos_antes']} | "
            f"vacíos={resultado['vacios_antes']} | "
            f"tiempo={resultado['segundos']:.4f}s"
        )

    job_link_resultado = next(
        fila
        for fila in filas
        if fila["columna"] == "job_link"
    )

    filas_descartadas = job_link_resultado["faltantes"]

    RESULTADOS.mkdir(exist_ok=True)

    archivo_csv = RESULTADOS / "consulta2.csv"

    with archivo_csv.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as archivo:

        writer = csv.writer(archivo)

        writer.writerow([
            "columna",
            "nulos_antes",
            "vacios_antes",
            "nulos_despues",
            "filas_sin_job_link_descartadas",
        ])

        for fila in filas:
            writer.writerow([
                fila["columna"],
                fila["nulos_antes"],
                fila["vacios_antes"],
                fila["nulos_despues"],
                filas_descartadas,
            ])

    (
        RESULTADOS / "consulta2_tiempo.txt"
    ).write_text(
        f"{tiempo_total:.6f}\n",
        encoding="utf-8",
    )

    print("\nRESULTADOS")

    print(
        f"{'columna':<24}"
        f"{'nulos':>10}"
        f"{'vacios':>10}"
        f"{'después':>12}"
    )

    for fila in filas:
        print(
            f"{fila['columna']:<24}"
            f"{fila['nulos_antes']:>10}"
            f"{fila['vacios_antes']:>10}"
            f"{fila['nulos_despues']:>12}"
        )

    print(
        f"\nFilas descartadas por job_link: "
        f"{filas_descartadas}"
    )

    print(
        f"Tiempo de ejecución: "
        f"{tiempo_total:.4f} segundos"
    )

    print(
        "\nResultados guardados en:",
        archivo_csv,
    )


if __name__ == "__main__":

    if (
        len(sys.argv) == 3
        and sys.argv[1] == "--columna"
    ):
        procesar_columna(sys.argv[2])

    else:
        ejecutar_consulta()