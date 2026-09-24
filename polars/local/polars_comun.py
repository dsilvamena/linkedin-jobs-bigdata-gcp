"""Rutas y operaciones compartidas por las consultas 2 a 10."""

from pathlib import Path

import polars as pl


RAIZ_PROYECTO = Path(__file__).resolve().parents[2]
DATOS = RAIZ_PROYECTO / "data_proyecto"
RESULTADOS = Path(__file__).resolve().parent / "resultados"


def leer(nombre: str) -> pl.LazyFrame:
    # Los campos de estos tres CSV son texto; evita inferencias distintas entre archivos.
    return pl.scan_csv(DATOS / nombre, infer_schema=False)


def ofertas_unicas() -> pl.LazyFrame:
    """Una oferta por enlace para que las publicaciones duplicadas no sesguen métricas."""
    return leer("linkedin_job_postings.csv").unique(subset="job_link", keep="first")


def lista_habilidades() -> pl.Expr:
    """Convierte 'a, b' en ['a', 'b']; los campos vacíos producen una lista vacía."""
    texto = pl.col("job_skills")
    return (
        pl.when(texto.is_null() | (texto.str.strip_chars() == ""))
        .then(pl.lit([]).cast(pl.List(pl.String)))
        .otherwise(texto.str.split(", "))
        .alias("habilidades")
    )


def guardar(numero: int, resultado: pl.DataFrame, segundos: float) -> None:
    RESULTADOS.mkdir(exist_ok=True)
    resultado.write_csv(RESULTADOS / f"consulta{numero}.csv")
    (RESULTADOS / f"consulta{numero}_tiempo.txt").write_text(
        f"{segundos:.6f}\n", encoding="utf-8"
    )
    print("\nRESULTADOS")
    print(resultado)
    print(f"Tiempo de ejecución: {segundos:.4f} segundos")
