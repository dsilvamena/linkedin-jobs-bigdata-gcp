"""Las diez consultas Dask, compartidas por la ejecución local y el clúster."""

from dataclasses import dataclass
from pathlib import Path
import time

import dask
import dask.dataframe as dd
import fsspec
import pandas as pd
from dask.distributed import get_client


@dataclass(frozen=True)
class Datos:
    carpeta: str
    storage_options: dict[str, str] | None = None
    resumen_parquet: str | None = None

    def ruta(self, nombre: str) -> str:
        return f"{self.carpeta.rstrip('/')}/{nombre}"

    def leer(self, nombre: str, columnas: list[str] | None = None) -> dd.DataFrame:
        return dd.read_csv(
            self.ruta(nombre),
            blocksize="64MB",
            dtype=str,
            usecols=columnas,
            storage_options=self.storage_options,
            keep_default_na=False,
        )

    def ofertas_unicas(self, columnas: list[str] | None = None) -> dd.DataFrame:
        columnas_lectura = None if columnas is None else list(dict.fromkeys(["job_link", *columnas]))
        return self.leer("linkedin_job_postings.csv", columnas_lectura).drop_duplicates(
            subset=["job_link"], keep="first"
        )


def _conteos_por_valor(datos: Datos, columna: str) -> pd.DataFrame:
    serie = datos.ofertas_unicas([columna])[columna].dropna()
    serie = serie[serie.str.strip() != ""]
    conteos = serie.value_counts(split_out=8).compute()
    return (
        conteos.rename_axis(columna)
        .reset_index(name="ofertas")
        .sort_values(["ofertas", columna], ascending=[False, True])
        .reset_index(drop=True)
    )


def consulta1(datos: Datos) -> tuple[pd.DataFrame, pd.DataFrame]:
    ofertas = datos.leer("linkedin_job_postings.csv")
    unicas = ofertas.drop_duplicates(subset=["job_link"], keep="first")
    antes, despues = dask.compute(ofertas.shape[0], unicas.shape[0])
    muestra = unicas[["job_link", "job_title", "company"]].head(5)
    resultado = pd.DataFrame([{
        "filas_originales": antes,
        "filas_sin_duplicados": despues,
        "duplicados_eliminados": antes - despues,
    }])
    return resultado, muestra


def consulta2(datos: Datos) -> tuple[pd.DataFrame, None]:
    ofertas = datos.leer("linkedin_job_postings.csv")
    columnas = list(ofertas.columns)
    # Todos los campos se leen como texto: blancos y espacios cuentan como nulos.
    normalizadas = ofertas.replace(r"^\s*$", pd.NA, regex=True)
    tratadas = normalizadas.dropna(subset=["job_link"]).fillna("No especificado")
    calculados = dask.compute(
        ofertas.isna().sum(),
        tratadas.isna().sum(),
        ofertas.shape[0],
        tratadas.shape[0],
        *[ofertas[c].fillna("").str.strip().eq("").sum() for c in columnas],
    )
    antes, despues, filas_antes, filas_despues = calculados[:4]
    vacios = dict(zip(columnas, calculados[4:]))
    resultado = pd.DataFrame({
        "columna": columnas,
        "nulos_antes": [int(antes[c]) for c in columnas],
        "vacios_antes": [int(vacios[c]) for c in columnas],
        "nulos_despues": [int(despues[c]) for c in columnas],
        "filas_sin_job_link_descartadas": filas_antes - filas_despues,
    })
    return resultado, None


def consulta3(datos: Datos) -> tuple[pd.DataFrame, pd.DataFrame]:
    habilidades = datos.leer("job_skills.csv", ["job_link", "job_skills"])
    texto = habilidades["job_skills"].fillna("")
    listas = texto.str.split(", ")
    cantidades = listas.str.len().where(texto.str.strip() != "", 0)
    filas, total = dask.compute(habilidades.shape[0], cantidades.sum())
    muestra = habilidades.head(5)[["job_link", "job_skills"]].copy()
    muestra["habilidades"] = muestra["job_skills"].fillna("").map(
        lambda valor: valor.split(", ") if valor.strip() else []
    )
    muestra = muestra[["job_link", "habilidades"]]
    return pd.DataFrame([{"filas": filas, "habilidades_totales": total}]), muestra


def consulta4(datos: Datos) -> tuple[pd.DataFrame, pd.DataFrame]:
    remotas = datos.ofertas_unicas(["job_title", "company", "job_type"])
    remotas = remotas[remotas["job_type"] == "Remote"]
    cantidad = remotas.shape[0].compute()
    muestra = remotas[["job_link", "job_title", "company", "job_type"]].head(5)
    return pd.DataFrame([{"ofertas_remotas": cantidad}]), muestra


def consulta5(datos: Datos) -> tuple[pd.DataFrame, None]:
    return _conteos_por_valor(datos, "company").head(10), None


def consulta6(datos: Datos) -> tuple[pd.DataFrame, None]:
    return _conteos_por_valor(datos, "search_country").head(10), None


def _longitudes_descripciones(datos: Datos) -> dd.DataFrame:
    if datos.resumen_parquet is not None:
        sistema, carpeta = fsspec.core.url_to_fs(
            datos.resumen_parquet, **(datos.storage_options or {})
        )
        if not sistema.exists(f"{carpeta.rstrip('/')}/_SUCCESS"):
            raise FileNotFoundError(
                "Falta el Parquet completo de job_summary. Ejecuta "
                "dask_proyecto/preparar_job_summary.py antes de la consulta 7."
            )
        descripciones = dd.read_parquet(
            f"{datos.resumen_parquet.rstrip('/')}/parte-*.parquet",
            columns=["job_link", "job_summary"],
            storage_options=datos.storage_options,
        )
        validas = descripciones[
            (descripciones["job_link"] != "") & (descripciones["job_summary"] != "")
        ]
        return validas.assign(
            longitud_caracteres=validas["job_summary"].str.len()
        )[["job_link", "longitud_caracteres"]]

    # Este CSV contiene saltos de línea dentro de campos entrecomillados.
    # read_csv de Dask no puede partirlo con seguridad por bloques. Pandas lo
    # recorre en chunks en el master; solo se envían enlace y longitud a workers.
    partes = []
    for chunk in pd.read_csv(
        datos.ruta("job_summary.csv"),
        usecols=["job_link", "job_summary"],
        dtype=str,
        chunksize=20_000,
        storage_options=datos.storage_options,
        keep_default_na=False,
    ):
        validas = chunk[(chunk["job_link"] != "") & (chunk["job_summary"] != "")]
        parte = pd.DataFrame({
            "job_link": validas["job_link"].to_numpy(),
            "longitud_caracteres": validas["job_summary"].str.len().to_numpy(),
        })
        parte["job_link"] = parte["job_link"].astype("string")
        partes.append(parte)
    # Scatter mantiene los datos en los workers sin incrustar ~145 MB en el grafo.
    futures = get_client().scatter(partes, broadcast=False)
    return dd.from_delayed(futures, meta=partes[0].iloc[:0])


def consulta7(datos: Datos) -> tuple[pd.DataFrame, None]:
    niveles = datos.ofertas_unicas(["job_level"])[["job_link", "job_level"]]
    niveles = niveles.dropna(subset=["job_level"])
    niveles = niveles[niveles["job_level"].str.strip() != ""]
    cruzadas = _longitudes_descripciones(datos).merge(niveles, on="job_link", how="inner")
    agregadas = cruzadas.groupby("job_level")["longitud_caracteres"].agg(
        ["count", "mean"]
    ).compute()
    resultado = (
        agregadas.rename(columns={"count": "descripciones", "mean": "promedio_caracteres"})
        .reset_index()
        .sort_values("job_level")
        .reset_index(drop=True)
    )
    resultado["descripciones"] = resultado["descripciones"].astype("int64")
    resultado["promedio_caracteres"] = (
        resultado["promedio_caracteres"].astype("float64").round(2)
    )
    return resultado, None


def consulta8(datos: Datos) -> tuple[pd.DataFrame, None]:
    habilidades = datos.leer("job_skills.csv", ["job_link", "job_skills"])
    habilidades = habilidades.drop_duplicates(subset=["job_link"], keep="first")
    separadas = habilidades["job_skills"].fillna("").str.split(", ").explode().str.strip()
    separadas = separadas[separadas != ""]
    frecuentes = separadas.value_counts(split_out=16).nlargest(20).compute()
    resultado = (
        frecuentes.rename_axis("habilidad")
        .reset_index(name="apariciones")
        .sort_values(["apariciones", "habilidad"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return resultado, None


def consulta9(datos: Datos) -> tuple[pd.DataFrame, None]:
    return _conteos_por_valor(datos, "company"), None


def consulta10(datos: Datos) -> tuple[pd.DataFrame, None]:
    habilidades = datos.leer("job_skills.csv", ["job_link", "job_skills"])
    habilidades = habilidades.drop_duplicates(subset=["job_link"], keep="first")
    texto = habilidades["job_skills"].fillna("")
    cantidades = texto.str.split(", ").str.len().where(texto.str.strip() != "", 0)
    conteos = habilidades[["job_link"]].assign(cantidad_habilidades=cantidades)
    ofertas = datos.ofertas_unicas()[["job_link"]]
    cruzadas = ofertas.merge(conteos, on="job_link", how="left")
    cantidad = cruzadas["cantidad_habilidades"].fillna(0)
    filas, con_habilidades, total = dask.compute(
        cruzadas.shape[0], (cantidad > 0).sum(), cantidad.sum()
    )
    return pd.DataFrame([{
        "ofertas": filas,
        "ofertas_con_habilidades": con_habilidades,
        "promedio_habilidades_por_oferta": round(total / filas, 2) if filas else None,
    }]), None


CONSULTAS = {
    1: consulta1,
    2: consulta2,
    3: consulta3,
    4: consulta4,
    5: consulta5,
    6: consulta6,
    7: consulta7,
    8: consulta8,
    9: consulta9,
    10: consulta10,
}


def ejecutar_consulta(numero: int, datos: Datos, resultados: Path) -> None:
    print(f"\n=== DASK - CONSULTA {numero} ===", flush=True)
    inicio = time.perf_counter()
    resultado, muestra = CONSULTAS[numero](datos)
    segundos = time.perf_counter() - inicio

    resultados.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(resultados / f"consulta{numero}.csv", index=False)
    (resultados / f"consulta{numero}_tiempo.txt").write_text(
        f"{segundos:.6f}\n", encoding="utf-8"
    )
    if muestra is not None:
        if numero == 3:
            muestra.to_json(
                resultados / "consulta3_muestra.json",
                orient="records",
                force_ascii=False,
            )
        else:
            muestra.to_csv(resultados / f"consulta{numero}_muestra.csv", index=False)

    print(resultado.head(20).to_string(index=False))
    print(f"Filas en el resultado: {len(resultado)}")
    print(f"Tiempo de ejecución: {segundos:.4f} segundos", flush=True)


def reunir_tiempos(resultados: Path) -> None:
    filas = [
        {
            "consulta": numero,
            "segundos": float(
                (resultados / f"consulta{numero}_tiempo.txt").read_text(
                    encoding="utf-8"
                ).strip()
            ),
        }
        for numero in range(1, 11)
    ]
    pd.DataFrame(filas).to_csv(resultados / "tiempos_dask.csv", index=False)
