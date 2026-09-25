"""Validacion de C8-C10 con Parquet local y Modin/Ray real."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pandas
import pyarrow as pa
import pyarrow.parquet as pq
import ray

import modin_comun as comun
import modin_consulta8 as c8
import modin_consulta9 as c9
import modin_consulta10 as c10
from modin.pandas.io import to_pandas


class ConsultasFinalesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Modin conserva referencias de Ray: compartir sesion durante la suite.
        if not ray.is_initialized():
            import atexit
            ray.init(num_cpus=2, include_dashboard=False, logging_level="ERROR")
            atexit.register(ray.shutdown)

    def setUp(self):
        self.temporal = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporal.cleanup)
        self.ruta = Path(self.temporal.name)
        self.skills = self.ruta / "skills"
        self.skills.mkdir()
        for numero, (links, textos) in enumerate([
            (["a", "b", "c", "d"], ["Python, SQL", "SQL, SQL", None, "  "]),
            (["a", "e", "f"], ["IGNORAR", "Go, , Rust", "C++,Python"]),
        ]):
            pq.write_table(pa.table({"job_link": links, "job_skills": textos}),
                           self.skills / f"parte-{numero}.parquet")
        self.postings = self.ruta / "postings.parquet"
        pq.write_table(pa.table({
            "job_link": ["a", "b", "c", "d", "e", "f", "z", "a"],
            "company": ["A", "B", "A", None, " ", "A", "A", "IGNORAR"],
        }), self.postings)

    def lotes(self, tamano=2):
        return comun.iterar_habilidades_unicas(self.skills.as_uri(), tamano)

    def test_deduplicacion_entre_archivos_y_lotes(self):
        for tamano in (1, 2, 10000):
            resultado = pandas.concat(list(self.lotes(tamano)), ignore_index=True)
            self.assertEqual(resultado.job_link.tolist(), list("abcdef"))
            self.assertEqual(resultado.iloc[0].job_skills, "Python, SQL")

    def test_c8_conteos_y_empates(self):
        self.assertEqual(c8.calcular(self.lotes()).to_dict("records"), [
            {"habilidad": "SQL", "apariciones": 3},
            {"habilidad": "C++,Python", "apariciones": 1},
            {"habilidad": "Go", "apariciones": 1},
            {"habilidad": "Python", "apariciones": 1},
            {"habilidad": "Rust", "apariciones": 1},
        ])

    def test_c8_top_global_no_top_por_lote(self):
        lotes = []
        for prefijo in ("A", "B"):
            texto = ", ".join([f"{prefijo}{i:02}" for i in range(21)] * 3 + ["Ganadora"] * 2)
            lotes.append(pandas.DataFrame({"job_link": [prefijo], "job_skills": [texto]}))
        resultado = c8.calcular(lotes)
        self.assertEqual(len(resultado), 20)
        self.assertEqual(resultado.iloc[0].to_dict(), {"habilidad": "Ganadora", "apariciones": 4})
        self.assertEqual(resultado.iloc[1:].habilidad.tolist(), [f"A{i:02}" for i in range(19)])

    def test_c8_unicode_nulos_y_concurrencia(self):
        textos = [None, "", "  ", "á, 😀", "\x1cSQL\x1f, \u2003Python\u3000", "SQL, SQL", "a,b"]
        lote = pandas.DataFrame({"job_link": list(map(str, range(len(textos)))), "job_skills": textos})
        valores = lote.job_skills.fillna("").str.split(", ").explode().str.strip()
        esperado = (valores[valores != ""].value_counts().rename_axis("habilidad")
                    .reset_index(name="apariciones")
                    .sort_values(["apariciones", "habilidad"], ascending=[False, True])
                    .reset_index(drop=True))
        for limite in (1, 2):
            obtenido = c8.calcular([lote.iloc[:3], lote.iloc[3:5], lote.iloc[5:]], limite)
            pandas.testing.assert_frame_equal(obtenido, esperado, check_dtype=False)

    def test_lector_enlaces_nulos_repetidos(self):
        for numero in (0, 1):
            pq.write_table(pa.table({"job_link": pa.array([None, "", "x"], type=pa.string()),
                                    "job_skills": [str(numero)] * 3}),
                           self.skills / f"parte-{numero}.parquet")
        resultado = pandas.concat(list(self.lotes(1)), ignore_index=True)
        self.assertEqual(resultado.job_link.tolist(), [None, "", "x"])
        self.assertEqual(resultado.job_skills.tolist(), ["0", "0", "0"])

    def test_c9_ranking_completo(self):
        ofertas = comun.pd.read_parquet(self.postings.as_posix())
        self.assertEqual(to_pandas(c9.calcular(ofertas)).to_dict("records"), [
            {"company": "A", "ofertas": 4}, {"company": "B", "ofertas": 1},
        ])
        ofertas = comun.pd.DataFrame({"job_link": list(range(25)), "company": [f"C{i:02}" for i in range(25)]})
        self.assertEqual(len(to_pandas(c9.calcular(ofertas))), 25)

    def test_c10_incluye_ofertas_sin_habilidades(self):
        conteos = c10.contar_habilidades(self.lotes())
        self.assertEqual(conteos.cantidad_habilidades.tolist(), [2, 2, 0, 0, 3, 1])
        ofertas = comun.pd.read_parquet(self.postings.as_posix(), columns=["job_link"])
        self.assertEqual(c10.calcular(ofertas, comun.pd.DataFrame(conteos)).to_dict("records"), [{
            "ofertas": 7, "ofertas_con_habilidades": 4, "promedio_habilidades_por_oferta": 1.14,
        }])

    def test_conteo_sin_listas_equivale_a_split(self):
        textos = [None, "", " ", "Python", "a,b", "a, b", ", ", "a, , b", "a,  b", "a, ", "á, 😀"]
        lote = pandas.DataFrame({"job_link": list(map(str, range(len(textos)))), "job_skills": textos})
        esperados = [len(t.split(", ")) if t and t.strip() else 0 for t in textos]
        self.assertEqual(c10.contar_habilidades([lote]).cantidad_habilidades.tolist(), esperados)

    def test_entradas_vacias(self):
        self.assertTrue(c8.calcular([]).empty)
        conteos = c10.contar_habilidades([])
        ofertas = comun.pd.DataFrame({"job_link": pandas.Series(dtype="object")})
        resultado = c10.calcular(ofertas, comun.pd.DataFrame(conteos))
        self.assertEqual(resultado.iloc[0].ofertas, 0)
        self.assertIsNone(resultado.iloc[0].promedio_habilidades_por_oferta)
        ofertas = comun.pd.DataFrame({"job_link": ["a"]})
        self.assertEqual(c10.calcular(ofertas, comun.pd.DataFrame(conteos)).iloc[0].promedio_habilidades_por_oferta, 0)

    def test_ejecucion_completa_y_archivos(self):
        for numero, modulo in ((8, c8), (9, c9), (10, c10)):
            with self.subTest(consulta=numero), \
                 patch.object(comun, "SKILLS_PARQUET", self.skills.as_uri()), \
                 patch.object(comun, "RESULTADOS", self.ruta / "resultados"), \
                 patch.object(modulo, "cerrar_ray"), \
                 patch.object(modulo, "POSTINGS_PARQUET", self.postings.as_posix(), create=True):
                modulo.main()
                salida = self.ruta / "resultados" / f"consulta{numero}.csv"
                self.assertFalse(pandas.read_csv(salida).empty)
                tiempo = salida.with_name(f"consulta{numero}_tiempo.txt")
                self.assertGreater(float(tiempo.read_text()), 0)


if __name__ == "__main__":
    unittest.main()
