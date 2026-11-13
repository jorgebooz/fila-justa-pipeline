"""Testes do Passo 8 sem Postgres (SQL real validado na execucao no Docker)."""
import importlib.util
import math
import pathlib
import tempfile
import unittest
from datetime import datetime

ARQUIVO = pathlib.Path(__file__).resolve().parents[1] / 'include/transform/equidade.py'
spec = importlib.util.spec_from_file_location('equidade', ARQUIVO)
equidade = importlib.util.module_from_spec(spec)
spec.loader.exec_module(equidade)


class TestEquidade(unittest.TestCase):
    def test_parametros_configurados(self):
        self.assertEqual(equidade.ler_parametros(), {'k': 10.0, 'alpha': 0.5, 'maximo': 2.0})

    def test_mesma_observacao_e_expectativa(self):
        self.assertEqual(equidade.fator_equidade(100, 100), 1)

    def test_mais_reportes_nao_penaliza(self):
        self.assertEqual(equidade.fator_equidade(200, 100), 1)

    def test_subrepresentacao_reforca(self):
        self.assertGreater(equidade.fator_equidade(20, 80), 1)

    def test_fator_tem_teto(self):
        self.assertEqual(equidade.fator_equidade(0, 10000), 2)

    def test_suavizacao_k(self):
        self.assertAlmostEqual(equidade.fator_equidade(0, 10), math.sqrt(2))

    def test_invalido(self):
        for kw in ({'observado': -1, 'esperado': 10},
                   {'observado': 0, 'esperado': -1},
                   {'observado': 0, 'esperado': 10, 'k': 0},
                   {'observado': 0, 'esperado': 10, 'maximo': 0.9}):
            with self.assertRaises(ValueError):
                equidade.fator_equidade(**kw)

    def test_intervalo_e_fuso(self):
        inicio, fim = equidade.intervalo('2026-01-01', '2027-01-01')
        self.assertLess(inicio, fim)
        self.assertEqual(inicio.utcoffset().total_seconds(), -3 * 3600)
        self.assertEqual(inicio.year, 2026)

    def test_intervalo_invalido(self):
        with self.assertRaises(ValueError):
            equidade.intervalo('2026-12-31', '2026-01-01')

    def test_yaml_invalido(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp)/'weights.yml'
            path.write_text('equidade_k: -1\nequidade_alpha: 0.5\nequidade_max: 2\n')
            with self.assertRaises(ValueError):
                equidade.ler_parametros(path)

    def test_sql_referencia_relato_antes_cluster(self):
        sql = equidade.SQL_FILE.read_text(encoding='utf-8')
        self.assertIn('silver.ocorrencia_limpa', sql)
        self.assertNotIn('silver.cluster_ocorrencia', sql)
        self.assertIn('stage_observado', sql)
        self.assertIn('POWER(', sql)
        self.assertIn('ON COMMIT DROP', sql)

    def test_sql_upsert_idempotente(self):
        self.assertIn('ON CONFLICT (area_id)', equidade.SQL_UPSERT_AREA)
        self.assertIn('ON CONFLICT (run_id, area_id)', equidade.SQL_UPSERT_EQUIDADE)
        self.assertIn('IS DISTINCT FROM', equidade.SQL_UPSERT_AREA)
        self.assertIn('IS DISTINCT FROM', equidade.SQL_UPSERT_EQUIDADE)


if __name__ == '__main__':
    unittest.main()
