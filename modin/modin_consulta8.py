"""
Consulta 8:
Top 20 habilidades más frecuentes.

Cada archivo Parquet se procesa en un proceso independiente
para liberar completamente la memoria entre partes.

El conteo global se mantiene en SQLite (disco), no en RAM.
Al final solo las 20 habilidades más frecuentes pasan a Modin.
"""

import sys
import sqlite3
import tempfile
import subprocess
import time
from collections import Counter
from pathlib import Path

import pyarrow.fs as fs
import pyarrow.parquet as pq


SKILLS_PARQUET = (
    "gs://utec-linkedin-jobs-2026/"
    "processed_partitioned/skills_parquet"
)


# ============================================================
# PROCESAR UNA SOLA PARTICIÓN
# ============================================================

def procesar_parte(ruta, ruta_db):

    sistema, archivo_path = fs.FileSystem.from_uri(ruta)

    conexion = sqlite3.connect(ruta_db)

    try:

        with sistema.open_input_file(archivo_path) as archivo:

            parquet = pq.ParquetFile(
                archivo,
                pre_buffer=False,
            )

            for lote in parquet.iter_batches(
                batch_size=2000,
                columns=[
                    "job_link",
                    "job_skills",
                ],
                use_threads=False,
            ):

                links = lote.column(
                    lote.schema.get_field_index(
                        "job_link"
                    )
                ).to_pylist()

                textos = lote.column(
                    lote.schema.get_field_index(
                        "job_skills"
                    )
                ).to_pylist()

                # Conteo únicamente del lote actual.
                contador = Counter()

                for link, texto in zip(
                    links,
                    textos,
                ):

                    # ------------------------------------------------
                    # Evitar job_link duplicados globalmente.
                    # ------------------------------------------------

                    clave = (
                        "\0"
                        if link is None
                        else "1" + str(link)
                    )

                    cursor = conexion.execute(
                        """
                        INSERT OR IGNORE
                        INTO vistos(job_link)
                        VALUES (?)
                        """,
                        (clave,),
                    )

                    # El job_link ya apareció anteriormente.
                    if cursor.rowcount == 0:
                        continue

                    # ------------------------------------------------
                    # Procesar habilidades.
                    # ------------------------------------------------

                    if texto is None:
                        continue

                    texto = str(texto).strip()

                    if not texto:
                        continue

                    for habilidad in texto.split(", "):

                        habilidad = habilidad.strip()

                        if habilidad:
                            contador[habilidad] += 1

                # ------------------------------------------------
                # Acumular el resultado del lote en SQLite.
                # ------------------------------------------------

                if contador:

                    conexion.executemany(
                        """
                        INSERT INTO conteos(
                            habilidad,
                            apariciones
                        )
                        VALUES (?, ?)

                        ON CONFLICT(habilidad)
                        DO UPDATE SET
                            apariciones =
                            apariciones +
                            excluded.apariciones
                        """,
                        contador.items(),
                    )

                conexion.commit()

                # El lote se descarta al continuar.
                del links
                del textos
                del contador

    finally:

        conexion.close()


# ============================================================
# LISTAR ARCHIVOS PARQUET
# ============================================================

def listar_partes():

    sistema, carpeta = fs.FileSystem.from_uri(
        SKILLS_PARQUET
    )

    archivos = sorted(
        info.path
        for info in sistema.get_file_info(
            fs.FileSelector(
                carpeta,
                recursive=True,
            )
        )
        if (
            info.is_file
            and info.path.endswith(".parquet")
        )
    )

    if not archivos:
        raise RuntimeError(
            "No se encontraron archivos Parquet "
            "de habilidades."
        )

    return [
        "gs://" + ruta
        for ruta in archivos
    ]


# ============================================================
# CONSULTA COMPLETA
# ============================================================

def ejecutar():

    print(
        "=== MODIN + ARROW - CONSULTA 8 ===",
        flush=True,
    )

    print(
        "Habilidades más frecuentes "
        "(explode + conteo)",
        flush=True,
    )

    inicio = time.perf_counter()

    partes = listar_partes()

    print(
        "Particiones encontradas:",
        len(partes),
        flush=True,
    )


    # ========================================================
    # BASE TEMPORAL EN DISCO
    # ========================================================

    with tempfile.TemporaryDirectory(
        prefix="consulta8_"
    ) as temporal:

        db = (
            Path(temporal) /
            "consulta8.sqlite"
        )

        conexion = sqlite3.connect(db)

        # Reduce escrituras auxiliares.
        conexion.execute(
            "PRAGMA journal_mode=OFF"
        )

        conexion.execute(
            "PRAGMA synchronous=OFF"
        )

        conexion.execute(
            "PRAGMA temp_store=FILE"
        )

        # Job links ya procesados.
        conexion.execute(
            """
            CREATE TABLE vistos (
                job_link TEXT PRIMARY KEY
            ) WITHOUT ROWID
            """
        )

        # Conteos globales de habilidades.
        conexion.execute(
            """
            CREATE TABLE conteos (
                habilidad TEXT PRIMARY KEY,
                apariciones INTEGER NOT NULL
            ) WITHOUT ROWID
            """
        )

        conexion.commit()
        conexion.close()


        # ====================================================
        # PROCESAR CADA PARQUET EN UN PROCESO INDEPENDIENTE
        # ====================================================

        for numero, parte in enumerate(
            partes,
            start=1,
        ):

            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),

                    "--parte",
                    parte,

                    "--db",
                    str(db),
                ],
                check=True,
            )

            print(
                f"Habilidades: parte "
                f"{numero}/{len(partes)} leida",
                flush=True,
            )


        # ====================================================
        # OBTENER TOP 20 DIRECTAMENTE DESDE SQLITE
        # ====================================================

        conexion = sqlite3.connect(db)

        filas = conexion.execute(
            """
            SELECT
                habilidad,
                apariciones

            FROM conteos

            ORDER BY
                apariciones DESC,
                habilidad ASC

            LIMIT 20
            """
        ).fetchall()

        conexion.close()


        # ====================================================
        # RESULTADO FINAL CON MODIN
        # ====================================================

        # Importamos Modin SOLO aquí.
        # Los subprocess anteriores nunca cargan Modin/Ray.
        from modin_comun import (
            pd,
            iniciar_ray,
            cerrar_ray,
            guardar,
        )

        from modin.pandas.io import (
            to_pandas
        )

        iniciar_ray()

        try:

            resultado = pd.DataFrame(
                filas,
                columns=[
                    "habilidad",
                    "apariciones",
                ],
            )

            # Orden final con Modin.
            resultado = (
                resultado
                .sort_values(
                    [
                        "apariciones",
                        "habilidad",
                    ],
                    ascending=[
                        False,
                        True,
                    ],
                )
                .reset_index(drop=True)
            )


            # =================================================
            # TRAER SOLO LAS 20 FILAS AL MASTER
            # =================================================

            resultado_local = to_pandas(
                resultado
            )


            # =================================================
            # TIEMPO
            # =================================================

            segundos = (
                time.perf_counter()
                - inicio
            )


            # =================================================
            # GUARDAR EN EL MASTER
            # =================================================

            guardar(
                8,
                resultado_local,
                segundos,
            )

        finally:

            cerrar_ray()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    # El mismo archivo funciona como worker de una sola parte.
    if (
        len(sys.argv) == 5
        and sys.argv[1] == "--parte"
        and sys.argv[3] == "--db"
    ):

        procesar_parte(
            sys.argv[2],
            sys.argv[4],
        )

    else:

        ejecutar()