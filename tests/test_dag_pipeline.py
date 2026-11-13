"""Testes do Passo 12 sem instalar Airflow no Python do Windows.

Testam estrutura, dependências, repasse da rodada e falhas dos subprocessos.
O parse real pelo Airflow deve ser validado dentro do Docker.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ARQUIVO_DAG = Path(__file__).resolve().parents[1] / 'dags/fila_justa_pipeline.py'


class Nó:
    def __init__(self, nome, arestas):
        self.nome = nome
        self.arestas = arestas

    def __rshift__(self, outro):
        self.arestas.append((self.nome, outro.nome))
        return outro


class CursorFalso:
    def __init__(self, resultados):
        self.resultados = iter(resultados)
        self.consultas = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, texto, parametros=None):
        self.consultas.append((texto, parametros))

    def fetchone(self):
        return next(self.resultados)


class ConexaoFalsa:
    def __init__(self, resultados):
        self.cursor_obj = CursorFalso(resultados)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return self.cursor_obj


class TestDagPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.funcoes = {}
        cls.arestas = []
        cls.atributos_dag = {}
        cls.contexto = {'run_id': 'manual__2026-11-12T21:00:00+00:00'}

        def task(**options):
            def decorator(funcao):
                cls.funcoes[options['task_id']] = funcao
                return lambda: Nó(options['task_id'], cls.arestas)
            return decorator

        def dag(**options):
            cls.atributos_dag.update(options)
            def decorator(funcao):
                return funcao
            return decorator

        airflow = types.ModuleType('airflow')
        sdk = types.ModuleType('airflow.sdk')
        sdk.task = task
        sdk.dag = dag
        sdk.get_current_context = lambda: cls.contexto
        pendulum = types.ModuleType('pendulum')
        pendulum.datetime = lambda *a, **kw: datetime(2026, 11, 12, tzinfo=timezone.utc)
        with patch.dict(sys.modules, {'airflow': airflow, 'airflow.sdk': sdk, 'pendulum': pendulum}):
            spec = importlib.util.spec_from_file_location('teste_dag_fila_justa', ARQUIVO_DAG)
            modulo = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(modulo)
        cls.modulo = modulo

    def test_total_tarefas_e_configuracao(self):
        self.assertEqual(len(self.funcoes), 13)
        self.assertEqual(self.atributos_dag['dag_id'], 'fila_justa_pipeline')
        self.assertEqual(self.atributos_dag['schedule'], '0 6 * * *')
        self.assertFalse(self.atributos_dag['catchup'])
        self.assertEqual(self.atributos_dag['max_active_runs'], 1)
        self.assertEqual(self.atributos_dag['default_args']['retries'], 2)

    def test_dependencias_obrigatorias(self):
        arestas = set(self.arestas)
        obrigatorias = {
            ('preparar_warehouse', 'garantir_pontos_distritais'),
            ('preparar_warehouse', 'ingestao_sp156'),
            ('preparar_warehouse', 'ingestao_ibge'),
            ('garantir_pontos_distritais', 'ingestao_clima'),
            ('garantir_pontos_distritais', 'ingestao_app_simulado'),
            ('ingestao_sp156', 'silver_padronizacao'),
            ('ingestao_ibge', 'silver_padronizacao'),
            ('ingestao_clima', 'silver_padronizacao'),
            ('ingestao_app_simulado', 'silver_padronizacao'),
            ('silver_padronizacao', 'silver_clusters'),
            ('silver_clusters', 'gold_areas_equidade'),
            ('gold_areas_equidade', 'silver_clima_48h'),
            ('silver_clima_48h', 'gold_score_fila'),
            ('gold_score_fila', 'validar_37_regras'),
            ('validar_37_regras', 'conferir_marts_publicados'),
        }
        self.assertEqual(arestas, obrigatorias)

    def test_run_id_estavel_e_distinto(self):
        f = self.modulo.id_rodada
        self.assertEqual(f('manual__a'), f('manual__a'))
        self.assertNotEqual(f('manual__a'), f('manual__b'))
        self.assertLess(len(f('manual__a')), 100)
        self.assertRaises(ValueError, f, ' ')

    def test_subprocesso_falha_encerra_tarefa(self):
        import subprocess
        with patch.object(self.modulo.subprocess, 'run', side_effect=subprocess.CalledProcessError(2, ['python'])) as p:
            with self.assertRaises(subprocess.CalledProcessError):
                self.modulo.executar_script('transform', 'score.py', '--run-id', 'teste')
            self.assertTrue(p.call_args.kwargs['check'])

    def test_argumentos_e_identificador_da_rodada(self):
        with patch.object(self.modulo, 'executar_script') as chamar:
            self.funcoes['gold_areas_equidade']()
            self.funcoes['gold_score_fila']()
            self.funcoes['validar_37_regras']()
        chamadas = chamar.call_args_list
        self.assertEqual(len(chamadas), 3)
        for c in chamadas:
            self.assertEqual(c.args[2:4], ('--run-id', self.modulo.rodada_atual()))
        self.assertIn('--inicio', chamadas[0].args)
        self.assertIn('--fim', chamadas[0].args)
        self.assertNotIn('--simular-falha', chamadas[-1].args)

    def test_bootstrap_preserva_views_gold_existentes(self):
        banco = ConexaoFalsa([('gold.fact_score',), (0,)])
        with patch.object(self.modulo, 'conexao_warehouse', return_value=banco):
            with patch.object(self.modulo, 'SQL_BASE', ARQUIVO_DAG.parents[1] / 'include/sql'):
                self.funcoes['preparar_warehouse']()
        sqls = [s for s, _ in banco.cursor_obj.consultas]
        self.assertEqual(len(sqls), 5)  # 3 DDLs + 2 verificações
        self.assertFalse(any('CREATE OR REPLACE VIEW gold.mart_' in x for x in sqls))

    def test_bootstrap_banco_vazio(self):
        banco = ConexaoFalsa([(None,)])
        with patch.object(self.modulo, 'conexao_warehouse', return_value=banco):
            with patch.object(self.modulo, 'SQL_BASE', ARQUIVO_DAG.parents[1] / 'include/sql'):
                self.funcoes['preparar_warehouse']()
        sqls = [s for s, _ in banco.cursor_obj.consultas]
        self.assertTrue(any('CREATE TABLE IF NOT EXISTS gold.fact_score' in x for x in sqls))

    def test_marts_rejeitam_rodada_errada(self):
        banco = ConexaoFalsa([(11,), (11, 11, 1, 'run_desatualizado', 1, 11), (11,), (11,)])
        with patch.object(self.modulo, 'conexao_warehouse', return_value=banco):
            with self.assertRaisesRegex(RuntimeError, 'Marts inconsistentes'):
                self.funcoes['conferir_marts_publicados']()

    def test_marts_aceitam_rodada_correta(self):
        run_id = self.modulo.rodada_atual()
        banco = ConexaoFalsa([(11,), (11, 11, 1, run_id, 1, 11), (11,), (11,)])
        with patch.object(self.modulo, 'conexao_warehouse', return_value=banco):
            self.funcoes['conferir_marts_publicados']()


if __name__ == '__main__':
    unittest.main()
