import sys
import json
import subprocess
from pathlib import Path

from modin_comun import (
    RESULTADOS,
    SKILLS_PARQUET,
)


def procesar_parte(ruta_parte):
    """
    Procesa UNA sola parte Parquet con Modin.
    Al terminar este proceso, Ray y toda su memoria desaparecen.
    """

    from modin_comun import (
        pd,
        iniciar_ray,
        cerrar_ray,
    )

    import time

    iniciar_ray()

    inicio = time.perf_counter()

    try:
        df = pd.read_parquet(
            ruta_parte,
            columns=[
                "job_link",
                "job_skills",
            ],
        )

        texto = df["job_skills"].fillna("")

        # Transformación solicitada:
        # "Python, SQL, AWS"
        # -> ["Python", "SQL", "AWS"]
        listas = texto.str.split(", ")

        cantidades = (
            listas
            .str.len()
            .where(
                texto.str.strip() != "",
                0,
            )
        )

        filas = len(df)

        habilidades_totales = int(
            cantidades.sum()
        )

        # Solo conservamos 5 filas como evidencia.
        pequena = df.head(5)

        links = (
            pequena["job_link"]
            .fillna("")
            .tolist()
        )

        textos = (
            pequena["job_skills"]
            .fillna("")
            .tolist()
        )

        muestra = []

        for link, habilidades in zip(
            links,
            textos,
        ):
            habilidades = str(habilidades)

            muestra.append({
                "job_link": str(link),
                "habilidades": (
                    habilidades.split(", ")
                    if habilidades.strip()
                    else []
                ),
            })

        segundos = (
            time.perf_counter()
            - inicio
        )

        resultado = {
            "filas": filas,
            "habilidades_totales": habilidades_totales,
            "segundos": segundos,
            "muestra": muestra,
        }

        print(
            "RESULT_JSON="
            + json.dumps(
                resultado,
                ensure_ascii=False,
            ),
            flush=True,
        )

    finally:
        cerrar_ray()


def listar_partes():
    """
    Obtiene todos los archivos .parquet
    del dataset particionado en GCS.
    """

    ruta = (
        SKILLS_PARQUET.rstrip("/")
        + "/*.parquet"
    )

    proceso = subprocess.run(
        [
            "gcloud",
            "storage",
            "ls",
            ruta,
        ],
        capture_output=True,
        text=True,
    )

    if proceso.returncode != 0:
        print(proceso.stderr)
        sys.exit(1)

    partes = [
        linea.strip()
        for linea in proceso.stdout.splitlines()
        if linea.strip().endswith(".parquet")
    ]

    if not partes:
        raise RuntimeError(
            "No se encontraron archivos Parquet "
            f"en {SKILLS_PARQUET}"
        )

    return partes


def ejecutar_consulta():

    print("=== MODIN - CONSULTA 3 ===")
    print("Transformar job_skills de texto a lista")
    print()

    partes = listar_partes()

    print(
        "Particiones encontradas:",
        len(partes),
    )

    total_filas = 0
    total_habilidades = 0
    tiempo_total = 0.0

    muestra_final = []

    for numero, parte in enumerate(
        partes,
        start=1,
    ):

        print(
            f"[{numero}/{len(partes)}] "
            f"Procesando partición...",
            flush=True,
        )

        proceso = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--parte",
                parte,
            ],
            capture_output=True,
            text=True,
        )

        if proceso.returncode != 0:
            print(
                "\nERROR EN PARTICIÓN:",
                parte,
            )

            print(proceso.stdout)
            print(proceso.stderr)

            sys.exit(1)

        resultado = None

        for linea in proceso.stdout.splitlines():

            if linea.startswith(
                "RESULT_JSON="
            ):
                resultado = json.loads(
                    linea.replace(
                        "RESULT_JSON=",
                        "",
                        1,
                    )
                )

        if resultado is None:
            raise RuntimeError(
                "No se obtuvo resultado "
                f"para {parte}"
            )

        total_filas += (
            resultado["filas"]
        )

        total_habilidades += (
            resultado[
                "habilidades_totales"
            ]
        )

        tiempo_total += (
            resultado["segundos"]
        )

        # Solo necesitamos 5 ejemplos
        if len(muestra_final) < 5:

            faltan = (
                5 - len(muestra_final)
            )

            muestra_final.extend(
                resultado["muestra"][
                    :faltan
                ]
            )

        print(
            f"    filas={resultado['filas']} | "
            f"habilidades="
            f"{resultado['habilidades_totales']} | "
            f"tiempo="
            f"{resultado['segundos']:.4f}s"
        )

    # -------------------------
    # Guardar resultados
    # -------------------------

    RESULTADOS.mkdir(
        exist_ok=True
    )

    import csv

    with (
        RESULTADOS /
        "consulta3.csv"
    ).open(
        "w",
        newline="",
        encoding="utf-8",
    ) as archivo:

        writer = csv.writer(
            archivo
        )

        writer.writerow([
            "filas",
            "habilidades_totales",
        ])

        writer.writerow([
            total_filas,
            total_habilidades,
        ])

    (
        RESULTADOS /
        "consulta3_tiempo.txt"
    ).write_text(
        f"{tiempo_total:.6f}\n",
        encoding="utf-8",
    )

    (
        RESULTADOS /
        "consulta3_muestra.json"
    ).write_text(
        json.dumps(
            muestra_final,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    # -------------------------
    # Mostrar resultado
    # -------------------------

    print("\nRESULTADOS")

    print(
        "Filas:",
        total_filas,
    )

    print(
        "Habilidades totales:",
        total_habilidades,
    )

    print(
        "\nPrimeras 5 listas:"
    )

    for fila in muestra_final:
        print(
            fila["job_link"],
            "->",
            fila["habilidades"],
        )

    print(
        f"\nTiempo de ejecución: "
        f"{tiempo_total:.4f} segundos"
    )

    print(
        "\nResultados guardados en:",
        RESULTADOS /
        "consulta3.csv",
    )


if __name__ == "__main__":

    if (
        len(sys.argv) == 3
        and sys.argv[1] == "--parte"
    ):

        procesar_parte(
            sys.argv[2]
        )

    else:

        ejecutar_consulta()