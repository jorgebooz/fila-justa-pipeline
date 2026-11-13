"""Testes sem banco do Passo 10. A execução real depende do PostgreSQL."""
import importlib.util
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('score', ROOT / 'include/transform/score.py')
score = importlib.util.module_from_spec(spec)
spec.loader.exec_module(score)


class TestScore(unittest.TestCase):
    def setUp(self):
        self.cfg = score.parametros()

    def test_pesos_configurados(self):
        self.assertEqual(self.cfg['w_risco'], Decimal(40))
        self.assertEqual(self.cfg['w_expo'], Decimal(30))
        self.assertEqual(self.cfg['w_tempo'], Decimal(10))
        self.assertEqual(self.cfg['w_clima'], Decimal(20))

    def test_score_zero_chuva_fator_um(self):
        r = score.score_referencia(1, 0, 0, 1, 1, 1, self.cfg)
        self.assertEqual(r['risco'], Decimal(40))
        self.assertEqual(r['expo'], Decimal('1.50000'))
        self.assertEqual(r['tempo'], Decimal(0))
        self.assertEqual(r['clima'], Decimal(0))
        self.assertEqual(r['score_com'], r['score_sem'])

    def test_equidade_afeta_somente_exposicao(self):
        a = score.score_referencia(8, 4, 7, '.7', '.6', 1, self.cfg)
        b = score.score_referencia(8, 4, 7, '.7', '.6', '1.5', self.cfg)
        for key in ['risco', 'tempo', 'clima', 'expo_sem', 'score_sem']:
            self.assertEqual(a[key], b[key])
        self.assertGreater(b['expo'], a['expo'])
        self.assertGreater(b['score_com'], a['score_com'])

    def test_componentes_somam_total(self):
        r = score.score_referencia(17, 13, 20, '.8', '.85', '1.4', self.cfg)
        self.assertEqual(r['score_com'], r['risco']+r['expo']+r['tempo']+r['clima'])
        self.assertEqual(r['score_sem'], r['risco']+r['expo_sem']+r['tempo']+r['clima'])

    def test_saturacao(self):
        a = score.score_referencia(20, 30, 50, 1, 1, 2, self.cfg)
        b = score.score_referencia(200, 300, 500, 1, 1, 2, self.cfg)
        self.assertEqual(a, b)
        self.assertEqual(a['score_0_100'], Decimal(100))

    def test_resultado_reproduzivel(self):
        args = (7, 19, '4.75', '.6', '.8', '1.35', self.cfg)
        self.assertEqual(score.score_referencia(*args), score.score_referencia(*args))

    def test_rejeita_dados_invalidos(self):
        for n, dias, chuva, risco, sens, fator in [
            (0, 1, 1, 1, 1, 1), (1, -1, 1, 1, 1, 1),
            (1, 1, -1, 1, 1, 1), (1, 1, 1, 1.2, 1, 1),
            (1, 1, 1, 1, 1, 0.8), (1, 1, 1, 1, 1, 2.1),
        ]:
            with self.subTest(n=n, dias=dias, chuva=chuva, risco=risco, fator=fator):
                with self.assertRaises(ValueError):
                    score.score_referencia(n, dias, chuva, risco, sens, fator, self.cfg)

    def test_seed_tipos_completo(self):
        tipos = score.ler_tipos()
        self.assertEqual(len(tipos), 5)
        self.assertEqual(len({x[0] for x in tipos}), 5)

    def test_config_invalida(self):
        with tempfile.TemporaryDirectory() as pasta:
            arq = Path(pasta)/'weights.yml'
            arq.write_text('w_risco: NaN\n', encoding='utf-8')
            with self.assertRaises(ValueError):
                score.parametros(arq)

    def test_sql_usa_ancora_cronologica_e_desempate(self):
        sql = score.STAGES.read_text(encoding='utf-8')
        self.assertIn('DISTINCT ON (c.cluster_id)', sql)
        self.assertIn('ORDER BY c.cluster_id, o.aberta_em, o.ocorrencia_id', sql)
        self.assertIn('COUNT(*)::integer AS n_relatos', sql)

    def test_sql_formula_e_ranks(self):
        sql = score.STAGES.read_text(encoding='utf-8')
        self.assertIn("current_setting('fila_justa.w_expo')", sql)
        self.assertIn('b.fator_equidade', sql)
        self.assertIn('ROW_NUMBER() OVER (ORDER BY p.score_sem_equidade DESC, p.cluster_id)', sql)
        self.assertIn('ROW_NUMBER() OVER (ORDER BY p.score_com_equidade DESC, p.cluster_id)', sql)

    def test_sql_conflict_nao_atualiza_igual(self):
        self.assertIn('ON CONFLICT (run_id, cluster_id) DO UPDATE', score.SQL_UPSERT_SCORE)
        self.assertIn('IS DISTINCT FROM', score.SQL_UPSERT_SCORE)
        self.assertIn('IS DISTINCT FROM', score.SQL_UPSERT_OCORRENCIAS)
        self.assertIn('ON CONFLICT (tipo_padrao) DO UPDATE', score.SQL_UPSERT_TIPOS)

    def test_views_isolam_rodada(self):
        self.assertEqual(score.SQL_MARTS.count('o.run_id = s.run_id'), 2)
        self.assertIn('AND e.run_id = s.run_id', score.SQL_MARTS)
        self.assertNotIn('ORDER BY MAX(avaliado_em)', score.SQL_MARTS)

    def test_historico_minimo_snapshot_adicionado(self):
        self.assertIn('ADD COLUMN IF NOT EXISTS avaliado_em', score.SQL_MIGRATION)
        self.assertIn('ADD COLUMN IF NOT EXISTS snapshot_clima', score.SQL_MIGRATION)

    def test_dry_run_nao_precisa_alterar_tabela(self):
        import inspect
        codigo = inspect.getsource(score.executar)
        self.assertIn('if not dry_run:', codigo)
        self.assertIn('cur.execute(SQL_MIGRATION)', codigo)
        self.assertLess(codigo.index('if not dry_run:'), codigo.index('cur.execute(SQL_MIGRATION)'))


if __name__ == '__main__':
    unittest.main()
