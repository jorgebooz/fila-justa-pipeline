"""Testes sem banco real para a tarefa de qualidade (Passo 11)."""
import importlib.util
import re
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

ARQUIVO = Path(__file__).resolve().parents[1] / 'include' / 'transform' / 'qualidade.py'
spec = importlib.util.spec_from_file_location('qualidade', ARQUIVO)
qualidade = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualidade)


class CursorFake:
    def __init__(self, linhas):
        self.linhas = linhas
        self.chamadas = []

    def execute(self, query, params=None):
        self.chamadas.append((query, params))

    def fetchall(self):
        return self.linhas


class TestQualidade(unittest.TestCase):
    def setUp(self):
        self.ok = [(regra, 0, 'ok') for regra in qualidade.REGRAS_OBRIGATORIAS]

    def test_todas_as_regras_passam(self):
        self.assertEqual(qualidade.avaliar(self.ok), [])

    def test_falha_no_score(self):
        linhas = self.ok.copy()
        linhas[24] = (linhas[24][0], 1, 'score inconsistente')
        self.assertEqual(qualidade.avaliar(linhas), [(linhas[24][0], 1, 'score inconsistente')])

    def test_falha_na_coordenada(self):
        linhas = self.ok.copy()
        linhas[17] = (linhas[17][0], 2, 'fora do municipio')
        self.assertEqual(len(qualidade.avaliar(linhas)), 1)

    def test_falta_regra_falha_fechado(self):
        with self.assertRaisesRegex(qualidade.FalhaQualidade, 'incompleto'):
            qualidade.avaliar(self.ok[1:])

    def test_regra_extra_falha_fechado(self):
        with self.assertRaisesRegex(qualidade.FalhaQualidade, 'inesperadas'):
            qualidade.avaliar(self.ok + [('outra', 0, 'ok')])

    def test_regra_duplicada_falha_fechado(self):
        with self.assertRaisesRegex(qualidade.FalhaQualidade, 'duplicadas'):
            qualidade.avaliar(self.ok + [self.ok[0]])

    def test_resultado_vazio_falha_fechado(self):
        with self.assertRaisesRegex(qualidade.FalhaQualidade, 'não retornou'):
            qualidade.avaliar([])

    def test_contagem_negativa_rejeitada(self):
        linhas = self.ok.copy()
        linhas[1] = (linhas[1][0], -1, 'erro')
        with self.assertRaises(qualidade.FalhaQualidade):
            qualidade.avaliar(linhas)

    def test_contagem_invalida_rejeitada(self):
        linhas = self.ok.copy()
        linhas[1] = (linhas[1][0], '1', 'erro')
        with self.assertRaises(qualidade.FalhaQualidade):
            qualidade.avaliar(linhas)

    def test_parametro_equidade(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / 'weights.yml'
            arquivo.write_text('equidade_max: 2.0 # teto\n', encoding='utf-8')
            self.assertEqual(qualidade.fator_maximo(arquivo), Decimal('2.0'))

    def test_equidade_duplicada_rejeitada(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / 'weights.yml'
            arquivo.write_text('equidade_max: 2\nequidade_max: 3\n', encoding='utf-8')
            with self.assertRaises(ValueError):
                qualidade.fator_maximo(arquivo)

    def test_equidade_invalida_rejeitada(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / 'weights.yml'
            arquivo.write_text('equidade_max: Infinity\n', encoding='utf-8')
            with self.assertRaises(ValueError):
                qualidade.fator_maximo(arquivo)

    def test_sql_contem_todas_as_regras(self):
        texto = qualidade.SQL.read_text(encoding='utf-8')
        identificadores = re.findall(r"SELECT\s+'(\d{2}_[a-z0-9_]+)'", texto)
        self.assertEqual(set(identificadores), set(qualidade.REGRAS_OBRIGATORIAS))
        self.assertEqual(len(identificadores), len(qualidade.REGRAS_OBRIGATORIAS))

    def test_sql_sem_escritas(self):
        texto = qualidade.SQL.read_text(encoding='utf-8')
        sem_comentarios = re.sub(r'--[^\n]*', '', texto)
        self.assertIsNone(re.search(r'\b(?:INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER)\b', sem_comentarios, re.I))

    def test_limite_e_run_id_parametrizados(self):
        texto = qualidade.SQL.read_text(encoding='utf-8')
        self.assertIn("current_setting('fila_justa.qa_run_id')", texto)
        self.assertIn("current_setting('fila_justa.qa_equidade_max')", texto)

    def test_executar_sql_sem_falhas(self):
        fake = CursorFake(self.ok)
        n = qualidade.executar_sql(fake, 'equidade_2026', Decimal('2.0'), verbose=False)
        self.assertEqual(n, 37)
        self.assertEqual(fake.chamadas[0][1], ('fila_justa.qa_run_id', 'equidade_2026'))
        self.assertEqual(fake.chamadas[1][1], ('fila_justa.qa_equidade_max', '2.0'))

    def test_executar_sql_com_falha_interrompe(self):
        linhas = self.ok.copy()
        linhas[29] = (linhas[29][0], 1, 'cluster sem Silver')
        fake = CursorFake(linhas)
        with self.assertRaisesRegex(qualidade.FalhaQualidade, '30_gold_cluster_pertence_silver'):
            qualidade.executar_sql(fake, 'equidade_2026', Decimal('2.0'), verbose=False)

    def test_run_id_vazio_rejeitado(self):
        with self.assertRaises(ValueError):
            qualidade.executar_sql(CursorFake(self.ok), ' ', Decimal('2'), verbose=False)

    def test_simulacao_usa_rollback_transacional(self):
        codigo = ARQUIVO.read_text(encoding='utf-8')
        self.assertIn('with conexao.transaction():', codigo)
        self.assertIn('raise _ReverterTeste', codigo)
        self.assertIn('comp_risco = s.comp_risco + 1', codigo)


if __name__ == '__main__':
    unittest.main()
