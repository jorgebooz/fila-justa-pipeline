"""Regressão: a fila deve mostrar a rodada atual, não o maior hash."""
import importlib.util
from pathlib import Path
import unittest

CAMINHO = Path(__file__).resolve().parents[1] / 'include/transform/score.py'

class TestPublicacaoMarts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location('score_publicacao', CAMINHO)
        modulo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(modulo)
        cls.sql = modulo.SQL_MARTS

    def test_duas_views_publicadas(self):
        self.assertIn('CREATE OR REPLACE VIEW gold.mart_fila_priorizacao', self.sql)
        self.assertIn('CREATE OR REPLACE VIEW gold.mart_comparacao_equidade', self.sql)

    def test_filtro_rodada_atual_em_ambas_as_views(self):
        self.assertEqual(self.sql.count('o.run_id = s.run_id'), 2)

    def test_nao_desempata_por_snapshot_ou_hash(self):
        self.assertNotIn('ORDER BY MAX(avaliado_em)', self.sql)
        self.assertNotIn('ORDER BY run_id DESC', self.sql)
        self.assertNotIn('JOIN rodada', self.sql)

    def test_comparacao_respeita_rodada_da_equidade(self):
        self.assertIn('e.run_id = s.run_id', self.sql)

    def test_filtro_preserva_componentes_e_contrato(self):
        for campo in ('score_0_100', 'comp_exposicao', 'comp_risco',
                      'comp_tempo', 'comp_clima', 'explicacao', 'rank_com'):
            self.assertIn(campo, self.sql)

if __name__ == '__main__':
    unittest.main()
