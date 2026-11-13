# Passo 8 — Áreas (IBGE) e fator de equidade

**Pré-requisitos:** Bronze IBGE (96 distritos), Silver (ocorrências padronizadas),
`include/config/weights.yml` e DDL `gold.dim_area` / `gold.fact_equidade_area`
já criado no Passo 3. Não substitua os arquivos existentes; esta entrega adiciona
apenas três arquivos (`include/transform/equidade.py`, `include/sql/06_areas_equidade.sql`,
`tests/test_equidade.py`) e este guia.

## Lógica

- `gold.dim_area`: 96 distritos com código oficial, nome, população e domicílios;
  o valor inicial `necessidade=1` representa uma **hipótese**, não indicador medido.
- `observado`: número de **relatos elegíveis** por distrito na `silver.ocorrencia_limpa`,
  antes da deduplicação (cada chamado conta, mesmo que vários pertençam ao mesmo
  cluster). Não é o total bruto indiscriminado do SP156: a Silver já excluiu
  serviços fora do escopo, datas inválidas e status não ativos.
- `esperado`: total dos relatos elegíveis × (população × necessidade) /
  soma(população × necessidade) entre os 96 distritos.
- `fator_equidade`: `min(max(((esperado+k)/(observado+k))**alpha, 1), M)`;
  `k=10`, `alpha=0.5` e `M=2` em `weights.yml`.
- Janela padrão: `[2026-01-01, 2027-01-01)` no horário de São Paulo.
  Inclui todos os relatos **disponíveis** de 2026 (e não presume que 2026
  inteiro tenha sido publicado). É possível passar `--inicio` e `--fim`.
- O fator apenas corrige a **exposição** no Passo 10; nunca altera o risco
  de vida por si só. Os fatores não são um retrato comprovado das necessidades
  reais, apenas a hipótese inicial do projeto.

## Testes locais (Git Bash)

```bash
python -m unittest discover -s tests -p 'test_equidade.py' -v
```

## Dry-run sem gravar Gold

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/equidade.py --dry-run
```

Esperado: `areas=96`, população 11.451.999, domicílios 4.996.529,
`fator_min>=1`, `fator_max<=2` e número de relatos elegíveis positivo.
A quantidade de relatos na janela é **medida na execução**, não predefinida.

## Carga real

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/equidade.py --run-id equidade_2026
```

Confirme com SQL:

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT COUNT(*) AS distritos, SUM(populacao) AS populacao,
       SUM(domicilios) AS domicilios
FROM gold.dim_area WHERE municipio_cod = '\''3550308'\'';
"'
```

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT COUNT(*) AS areas, SUM(observado) AS observado,
       ROUND(SUM(esperado), 0) AS esperado,
       MIN(fator_equidade) AS fator_min,
       MAX(fator_equidade) AS fator_max,
       COUNT(*) FILTER (WHERE fator_equidade > 1) AS areas_reforcadas
FROM gold.fact_equidade_area
WHERE run_id = '\''equidade_2026'\'';
"'
```

Esperado: 96 áreas; somas observado/esperado equivalentes (arredondamento);
fator em `[1, 2]` e ao menos uma área reforçada.

## Idempotência

Execute a carga real **outra vez** com o mesmo `--run-id` e a mesma janela.
Sem alteração das fontes, espera-se:
`dim_area inseridas_ou_alteradas=0 | fact_equidade_area inseridas_ou_alteradas=0`.

## Validação pendente do Passo 7

Antes do Passo 8, repita `clusters.py --run-id clusters_2026` para verificar
`inseridas_ou_alteradas=0`. Também pode verificar clusters com relatos de
vários distritos. O Passo 8 calcula a equidade sobre **relatos por distrito**,
sem precisar escolher uma área para clusters que cruzam limites. O Passo 10
precisará de uma regra determinística para atribuir área aos fatos por cluster.

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT COUNT(*) AS clusters_multiplos_distritos FROM (
  SELECT c.cluster_id FROM silver.cluster_ocorrencia c
  JOIN silver.ocorrencia_limpa o USING (ocorrencia_id)
  GROUP BY c.cluster_id HAVING COUNT(DISTINCT o.area_id) > 1
) x;
"'
```

**Limitações:** só inclui o subconjunto elegível da Silver; população de
2022 para dados de 2026; parâmetro `necessidade=1`; não realiza junção espacial
para registros SP156 sem distrito ou com código numérico não conciliado.
