# Correção da publicação dos marts (Passo 12)

## Sintoma

Regra `28_gold_mart_rodada_correta` acusou 11.163 violações depois que
`gold_score_fila` terminou, ao validar `run_id` diferente do exposto pela view.

## Causa

O código antigo selecionava `ORDER BY MAX(avaliado_em) DESC, run_id DESC`.
`avaliado_em` representa o horário do **snapshot climático**, não o instante
da publicação. Duas execuções que usam a mesma hora recebem o mesmo snapshot,
e o hash `run_id` não ordena as execuções temporalmente. A DAG **já** contém
`max_active_runs=1`; o problema não depende de paralelismo.

## Ajuste

As duas views selecionam `gold.fact_score` apenas quando
`gold.fact_ocorrencia.run_id = gold.fact_score.run_id`. O script do score
já atualiza `fact_ocorrencia.run_id` para cada cluster na mesma transação
que grava os scores. Nenhuma tabela é apagada nem renomeada.

## Aplicar no banco existente

1. Pare/dispare somente após terminar as execuções de DAG em andamento.
2. Teste: `python -m unittest discover -s tests -p "test_publicacao_marts.py" -v`.
3. Aplique o SQL em `include/sql/10_corrigir_publicacao_marts.sql` via psql.
4. Confira `run_id` e contagem dos marts e dos fatos.
5. Execute a qualidade com o `run_id` encontrado na `gold.fact_ocorrencia`.
6. Dispare uma DAG manual e valide suas 13 tarefas.

### Limitação

`gold.fact_ocorrencia` é a fotografia corrente por cluster. Como o script de
score não remove fatos de clusters excluídos em rodadas futuras, o processo
deverá ganhar reconciliação quando o conjunto de clusters mudar. Uma futura
tabela explícita de publicação por `run_id` será mais robusta para histórico
completo e reprocessamentos retroativos.
