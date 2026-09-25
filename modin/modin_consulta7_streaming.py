import time
import gc

import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from modin_comun import (
    pd,
    POSTINGS_PARQUET,
    iniciar_ray,
    cerrar_ray,
    guardar,
)


print("=== MODIN - CONSULTA 7 ===")
print("Longitud promedio de la descripción por job_level")

inicio = time.perf_counter()


# ============================================================
# 1. MODIN: obtener job_link -> job_level
# ============================================================

print("Cargando niveles con Modin...")

iniciar_ray()

niveles = pd.read_parquet(
    POSTINGS_PARQUET,
    columns=[
        "job_link",
        "job_level",
    ],
)

niveles = niveles.dropna(
    subset=[
        "job_link",
        "job_level",
    ]
)

niveles = niveles[
    (niveles["job_link"].str.strip() != "")
    & (niveles["job_level"].str.strip() != "")
]

# Consulta 1 confirmó que no hay job_link duplicados.
links = niveles["job_link"].tolist()
levels = niveles["job_level"].tolist()

mapa_niveles = dict(
    zip(links, levels)
)

print(
    "Ofertas con nivel válido:",
    len(mapa_niveles),
)

# Ya no necesitamos el DataFrame distribuido
del niveles
del links
del levels

# Solo desconecta ESTE script.
# El clúster Ray sigue encendido.
cerrar_ray()

gc.collect()


# ============================================================
# 2. PYARROW: recorrer job_summary sin acumularlo
# ============================================================

print("\nProcesando job_summary por streaming...")

gcs = pafs.GcsFileSystem()

carpeta = (
    "utec-linkedin-jobs-2026/"
    "processed/job_summary_parquet"
)

selector = pafs.FileSelector(
    carpeta,
    recursive=True,
)

archivos = [
    info.path
    for info in gcs.get_file_info(selector)
    if info.is_file
    and info.path.endswith(".parquet")
]

print(
    "Particiones encontradas:",
    len(archivos),
)


# ============================================================
# 3. Acumuladores mínimos
# ============================================================

acumulados = {}

total_lotes = 0
total_descripciones = 0
total_cruzadas = 0


# ============================================================
# 4. Procesar UN lote y descartarlo
# ============================================================

for numero_archivo, ruta in enumerate(
    archivos,
    start=1,
):

    with gcs.open_input_file(ruta) as archivo:

        parquet = pq.ParquetFile(
            archivo
        )

        for batch in parquet.iter_batches(
            batch_size=2000,
            columns=[
                "job_link",
                "job_summary",
            ],
            use_threads=False,
        ):

            total_lotes += 1

            links_batch = batch.column(
                batch.schema.get_field_index(
                    "job_link"
                )
            )

            summary_batch = batch.column(
                batch.schema.get_field_index(
                    "job_summary"
                )
            )

            # PyArrow calcula longitud sin convertir
            # las descripciones a objetos Python.
            longitudes = pc.utf8_length(
                summary_batch
            )

            # Solo pasan a Python:
            # job_link + número entero
            links_python = (
                links_batch.to_pylist()
            )

            longitudes_python = (
                longitudes.to_pylist()
            )

            for link, longitud in zip(
                links_python,
                longitudes_python,
            ):

                if (
                    not link
                    or longitud is None
                    or longitud == 0
                ):
                    continue

                nivel = mapa_niveles.get(
                    link
                )

                if not nivel:
                    continue

                nivel = str(nivel).strip()

                if not nivel:
                    continue

                if nivel not in acumulados:
                    acumulados[nivel] = {
                        "count": 0,
                        "sum": 0,
                    }

                acumulados[nivel][
                    "count"
                ] += 1

                acumulados[nivel][
                    "sum"
                ] += int(longitud)

                total_cruzadas += 1

            total_descripciones += len(
                links_python
            )

            # Este lote desaparece completamente
            del batch
            del links_batch
            del summary_batch
            del longitudes
            del links_python
            del longitudes_python

        print(
            f"Partición "
            f"{numero_archivo}/{len(archivos)} "
            f"completada",
            flush=True,
        )

    gc.collect()


# ============================================================
# 5. Resultado final
# ============================================================

filas = []

for nivel in sorted(acumulados):

    cantidad = (
        acumulados[nivel]["count"]
    )

    suma = (
        acumulados[nivel]["sum"]
    )

    filas.append({
        "job_level": nivel,
        "descripciones": cantidad,
        "promedio_caracteres": round(
            suma / cantidad,
            2,
        ),
    })


# ============================================================
# 6. MODIN: resultado final
# ============================================================

iniciar_ray()

resultado = pd.DataFrame(
    filas
)

segundos = (
    time.perf_counter()
    - inicio
)

guardar(
    7,
    resultado,
    segundos,
)

print(
    "\nDescripciones recorridas:",
    total_descripciones,
)

print(
    "Descripciones cruzadas:",
    total_cruzadas,
)

print(
    "Lotes procesados:",
    total_lotes,
)

cerrar_ray()