"""Ejecuta las diez consultas desde el master en los workers remotos."""

from pathlib import Path

from inicio import DATOS, RESULTADOS, cliente, ejecutar_consulta, reunir_tiempos


def reunir_tiempos_txt(resultados: Path) -> Path:
    """Reúne los tiempos mostrados al terminar cada consulta."""
    reporte = resultados / "tiempos_consultas.txt"
    with reporte.open("w", encoding="utf-8") as salida:
        salida.write("=== TIEMPOS DASK ===\n")
        for numero in range(1, 11):
            archivo = resultados / f"consulta{numero}_tiempo.txt"
            segundos = float(archivo.read_text(encoding="utf-8").strip())
            salida.write(f"Consulta {numero}: {segundos:.4f} segundos\n")
    return reporte


def main() -> None:
    with cliente():
        for numero in range(1, 11):
            ejecutar_consulta(numero, DATOS, RESULTADOS)
    reunir_tiempos(RESULTADOS)
    reporte = reunir_tiempos_txt(RESULTADOS)
    print("\nTiempos guardados en", RESULTADOS / "tiempos_dask.csv")
    print("Resumen de tiempos guardado en", reporte)


if __name__ == "__main__":
    main()
