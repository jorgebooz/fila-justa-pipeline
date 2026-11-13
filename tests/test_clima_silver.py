"""Testes locais do Passo 9 sem exigir PostgreSQL nem internet."""
import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path

PATH = Path(__file__).resolve().parents[1] / 'include/transform/clima_silver.py'
spec = importlib.util.spec_from_file_location('clima_silver', PATH)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class TestClimaSilver(unittest.TestCase):
    def test_snapshot_utc(self):
        self.assertEqual(mod.parse_snapshot('2026-11-12T02:00:00Z'),
                         datetime(2026, 11, 12, 2, tzinfo=timezone.utc))

    def test_snapshot_com_offset(self):
        self.assertEqual(mod.parse_snapshot('2026-11-11T23:00:00-03:00'),
                         datetime(2026, 11, 12, 2, tzinfo=timezone.utc))

    def test_snapshot_sem_fuso_rejeitado(self):
        with self.assertRaises(ValueError):
            mod.parse_snapshot('2026-11-12T02:00:00')

    def test_metricas_esperadas(self):
        mod.verificar_metricas(4608, 96, 48, 0)

    def test_metricas_incompletas(self):
        for args in ((4607, 96, 48, 0), (4608, 95, 48, 0),
                     (4608, 96, 47, 0), (4608, 96, 48, 1)):
            with self.subTest(args=args), self.assertRaises(RuntimeError):
                mod.verificar_metricas(*args)

    def test_upsert_idempotente(self):
        self.assertIn('ON CONFLICT (area_id, hora, consultado_em)', mod.SQL_UPSERT)
        self.assertIn('IS DISTINCT FROM', mod.SQL_UPSERT)

    def test_view_por_snapshot_mais_recente(self):
        self.assertIn('MAX(consultado_em)', mod.SQL_VIEW)
        self.assertIn('INTERVAL \'48 hours\'', mod.SQL_VIEW)

    def test_sql_elt_no_warehouse(self):
        sql = mod.SQL_FILE.read_text(encoding='utf-8')
        self.assertIn('CREATE TEMP TABLE stage_clima_previsao', sql)
        self.assertIn('precip_mm_raw::numeric', sql)


if __name__ == '__main__':
    unittest.main()
