"""Passo 11: validação de qualidade read-only para Bronze, Silver e Gold.

Uso no Airflow scheduler, depois de terminar score.py:
  python -u /opt/airflow/include/transform/qualidade.py --run-id equidade_2026

A execução normal apenas CONSULTA o banco; não cria tabelas nem altera dados.
Retorna código de saída 1 quando encontra qualquer violação, para falhar a DAG.

Para demonstrar detecção, --simular-falha altera temporariamente uma linha de
fact_score dentro de uma TRANSAÇÃO SEM COMMIT, detecta e reverte via rollback.
Não execute essa opção em uma DAG operacional.

limitação: as 96 áreas e as 48 horas são uma regra fixa do MVP de São Paulo.
Evolução: parametrizar esses limites por município e horizonte de previsão.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
SQL = RAIZ / 'include' / 'sql' / '09_qualidade.sql'
PESOS = RAIZ / 'include' / 'config' / 'weights.yml'

# Permite detectar exclusões acidentais das validações durante manutenção.
REGRAS_OBRIGATORIAS = tuple(
    f'{numero:02d}_{sufixo}'
    for numero, sufixo in enumerate((
        'bronze_sp156_nao_vazia',
        'bronze_ibge_96',
        'bronze_clima_nao_vazia',
        'bronze_app_nao_vazia',
        'silver_ocorrencia_nao_vazia',
        'silver_cluster_cobre_ocorrencias',
        'silver_clima_nao_vazia',
        'gold_dim_area_96',
        'gold_dim_tipo_nao_vazia',
        'gold_dim_tempo_nao_vazia',
        'gold_fact_ocorrencia_vs_clusters',
        'gold_equidade_96_por_rodada',
        'gold_score_vs_fact_ocorrencia',
        'mart_fila_vs_score',
        'mart_equidade_vs_score',
        'bronze_sp156_chaves_unicas',
        'bronze_app_chaves_unicas',
        'silver_ocorrencia_campos_validos',
        'silver_cluster_chaves_unicas',
        'silver_cluster_tipo_h3_consistente',
        'silver_clusters_cobertura_referencial',
        'gold_dim_area_valores',
        'gold_equidade_limites',
        'gold_equidade_soma_relatos',
        'gold_score_valores_e_formula',
        'gold_score_explicacao',
        'gold_rank_contiguo',
        'gold_mart_rodada_correta',
        'gold_fila_componentes',
        'gold_cluster_pertence_silver',
        'gold_fact_soma_relatos',
        'gold_cluster_count_distinto',
        'silver_snapshot_clima_atualizado',
        'silver_clima_96x48',
        'silver_clima_horas_por_area',
        'silver_clima_sem_valores_invalidos',
        'gold_score_snapshot_corresponde_clima',
    ), start=1)
)


class FalhaQualidade(RuntimeError):
    """Alguma regra obrigatória falhou ou não foi executada."""


class _ReverterTeste(Exception):
    """Força rollback da simulação controlada, sem commit."""


def fator_maximo(caminho=PESOS):
    """Lê o limite M do arquivo já versionado, sem depender de PyYAML."""
    linhas = Path(caminho).read_text(encoding='utf-8').splitlines()
    encontrados = []
    for linha in linhas:
        conteudo = linha.split('#', 1)[0].strip()
        if re.match(r'^equidade_max\s*:', conteudo):
            encontrados.append(conteudo.split(':', 1)[1].strip())
    if len(encontrados) != 1:
        raise ValueError('weights.yml deve conter uma única entrada equidade_max')
    try:
        valor = Decimal(encontrados[0])
    except InvalidOperation as exc:
        raise ValueError('equidade_max inválido em weights.yml') from exc
    if not valor.is_finite() or not (Decimal('1') <= valor <= Decimal('5')):
        raise ValueError('equidade_max precisa estar entre 1 e 5')
    return valor


def avaliar(linhas):
    """Confere que TODAS as regras retornaram uma vez e que não há falhas."""
    if not linhas:
        raise FalhaQualidade('Consulta de qualidade não retornou verificações')
    nomes = [regra for regra, _, _ in linhas]
    faltando = set(REGRAS_OBRIGATORIAS) - set(nomes)
    extras = set(nomes) - set(REGRAS_OBRIGATORIAS)
    if len(nomes) != len(set(nomes)) or faltando or extras:
        raise FalhaQualidade(
            f'Conjunto de regras incompleto/inconsistente: faltando={sorted(faltando)}, '
            f'inesperadas={sorted(extras)}, duplicadas={len(nomes) != len(set(nomes))}'
        )
    falhas = []
    for regra, quantidade, detalhe in linhas:
        if isinstance(quantidade, bool) or not isinstance(quantidade, int) or quantidade < 0:
            raise FalhaQualidade(f'Regra {regra} retornou quantidade inválida: {quantidade!r}')
        if quantidade > 0:
            falhas.append((regra, quantidade, str(detalhe)))
    return falhas


def executar_sql(cur, run_id, limite, verbose=True):
    if not run_id or len(run_id) > 200 or not run_id.strip():
        raise ValueError('run_id obrigatório, tamanho máximo 200')
    cur.execute('SELECT set_config(%s,%s,true)', ('fila_justa.qa_run_id', run_id))
    cur.execute('SELECT set_config(%s,%s,true)', ('fila_justa.qa_equidade_max', str(limite)))
    cur.execute(SQL.read_text(encoding='utf-8'))
    linhas = cur.fetchall()
    falhas = avaliar(linhas)
    if verbose:
        for regra, quantidade, detalhe in linhas:
            situacao = 'OK' if quantidade == 0 else 'FALHOU'
            print(f'{situacao}: {regra} | falhas={quantidade} | {detalhe}', flush=True)
        print(f'QUALIDADE: regras={len(linhas)} | falhas={len(falhas)} | run_id={run_id}', flush=True)
    if falhas:
        raise FalhaQualidade('\n'.join(
            f'{regra}: {quantidade} violação(ões); {detalhe}'
            for regra, quantidade, detalhe in falhas
        ))
    return len(linhas)


def validar_conexao(conexao, run_id, limite):
    with conexao.transaction():
        with conexao.cursor() as cur:
            return executar_sql(cur, run_id, limite)


def simular_violacao(conexao, run_id, limite):
    """Teste negativo real do SQL, alterando uma linha apenas sem commit."""
    try:
        with conexao.transaction():
            with conexao.cursor() as cur:
                cur.execute('''
                    UPDATE gold.fact_score s SET comp_risco = s.comp_risco + 1
                    WHERE (s.run_id, s.cluster_id) = (
                        SELECT run_id, cluster_id FROM gold.fact_score
                        WHERE run_id = %s ORDER BY cluster_id LIMIT 1
                    )
                    RETURNING cluster_id
                ''', (run_id,))
                modificado = cur.fetchone()
                if not modificado:
                    raise FalhaQualidade('Não há score para simular violação')
                try:
                    executar_sql(cur, run_id, limite, verbose=False)
                except FalhaQualidade as erro:
                    if '25_gold_score_valores_e_formula' not in str(erro):
                        raise FalhaQualidade('Falha simulada não foi identificada na regra de score') from erro
                    print(f'FALHA ESPERADA: regra de score detectou alteração temporária em {modificado[0]}', flush=True)
                    raise _ReverterTeste from erro
                raise FalhaQualidade('ERRO: violação injetada não foi detectada')
    except _ReverterTeste:
        print('OK: transação revertida integralmente (ROLLBACK); banco preservado', flush=True)
        return True


def main(argv=None):
    ap = argparse.ArgumentParser(description='Passo 11: qualidade Bronze -> Silver -> Gold')
    ap.add_argument('--run-id', default='equidade_2026',
                    help='Mesma rodada de score e equidade; use ID dinâmico na DAG')
    ap.add_argument('--simular-falha', action='store_true',
                    help='Teste negativo reversível; não use em execução operacional')
    args = ap.parse_args(argv)
    dsn = os.environ.get('AIRFLOW_CONN_WAREHOUSE')
    if not dsn:
        raise SystemExit('ERRO: AIRFLOW_CONN_WAREHOUSE não configurada')
    limite = fator_maximo()
    import psycopg
    try:
        with psycopg.connect(dsn) as conexao:
            if args.simular_falha:
                if not simular_violacao(conexao, args.run_id, limite):
                    raise FalhaQualidade('Teste negativo não comprovado')
            else:
                total = validar_conexao(conexao, args.run_id, limite)
                print(f'OK: {total} verificações de qualidade passaram; a DAG pode prosseguir', flush=True)
    except (FalhaQualidade, ValueError) as erro:
        print(f'ERRO DE QUALIDADE: {erro}', file=sys.stderr, flush=True)
        raise SystemExit(1) from erro


if __name__ == '__main__':
    main()
