# Passo 7 — Deduplicação de ocorrências (H3 + tipo + janela)

## Pré-requisito

Passo 6 executado e validado: `silver.ocorrencia_limpa` com os 16.553 registros
informados na validação local (15.664 SP156 + 889 simulados), sem nulos nas chaves.
Não execute o DDL novamente e não altere os conectores já validados.

## Regra de agrupamento

- Mesmo `tipo_padrao` e **mesma** célula `h3` (já calculada na Silver, resolução 9).
- Dentro de `cluster_janela_dias: 7`, lido de `include/config/weights.yml`.
- A janela é contada do **primeiro relato do cluster**, não da ocorrência
  imediatamente anterior (evita encadeamento sem limite temporal).
- Empates de horário são desempados por `ocorrencia_id`; a identificação do cluster
  vem de `md5(tipo_padrao:h3:ocorrencia_id_ancora)` com prefixo `cl_`.
- Cada ocorrência permanece na Silver; a associação `ocorrencia_id -> cluster_id`
  é registrada em `silver.cluster_ocorrencia`.
- Reexecutar sem mudanças mantém IDs, contagem e `run_id` das linhas inalteradas.
- Descartes são gerados **no Passo 6**, não aqui.

**Limitações:** dois relatos em células H3 vizinhas não são unidos, mesmo que estejam
perto. Uma célula H3 também pode atravessar a fronteira entre distritos; no Passo 8
será necessário definir uma regra determinística de área para o cluster e conferir
se houve clusters com relatos de áreas diferentes. Novos relatos com abertura mais
antiga podem alterar a âncora e o ID do cluster; ao construir Gold, o processamento
completo precisa reconciliar os fatos dependentes. Os hashes não substituem uma
identidade oficial de ocorrência.

## Arquivos novos

- `include/sql/05_clusters.sql`
- `include/transform/clusters.py`
- `tests/test_clusters.py`
- `docs/VALIDACAO_PASSO7_CLUSTERS.md`

Nenhum arquivo existente deve ser substituído. É necessário que o volume
`./include:/opt/airflow/include` já esteja montado no scheduler (como no Passo 6).

## 1. Testes unitários (Git Bash)

```bash
python -m unittest discover -s tests -p 'test_clusters.py' -v
```

## 2. Simulação sobre dados reais, sem gravar

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/clusters.py --dry-run
```

A saída deve apresentar `Silver=16553`, número de clusters calculado e
`grupos_divididos=0` para o gerador simulado. O número final de clusters precisa
ser observado no seu PostgreSQL; não foi inventado no pacote.

## 3. Executar carga de associações

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/clusters.py --run-id clusters_2026
```

## 4. Validar associações, duplicações e distribuição

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT COUNT(*) AS vinculos,
       COUNT(DISTINCT ocorrencia_id) AS ocorrencias_unicas,
       COUNT(DISTINCT cluster_id) AS clusters
FROM silver.cluster_ocorrencia;
"'
```

`vinculos=ocorrencias_unicas=16553`. Clusters devem ser **menos** que 16.553
se a deduplicação encontrou mais de uma ocorrência no mesmo grupo.

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT o.canal, COUNT(*) AS relatos,
       COUNT(DISTINCT c.cluster_id) AS clusters_com_canal
FROM silver.ocorrencia_limpa o
JOIN silver.cluster_ocorrencia c USING(ocorrencia_id)
GROUP BY o.canal ORDER BY o.canal;
"'
```

Verifique que todos os 889 registros APP_SIMULADO têm vínculo e que o script
informou `grupos_divididos=0`.

### Conferir clusters que cruzam distritos (diagnóstico para Passo 8)

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT COUNT(*) AS clusters_multiplos_distritos FROM (
    SELECT c.cluster_id
    FROM silver.cluster_ocorrencia c
    JOIN silver.ocorrencia_limpa o USING (ocorrencia_id)
    GROUP BY c.cluster_id HAVING COUNT(DISTINCT o.area_id) > 1
) x;
"'
```

Caso não seja zero, anote o número. Não excluímos esses relatos silenciosamente.

## 5. Idempotência

Repita o comando do item 3. Com a Silver inalterada, esperamos
`inseridas_ou_alteradas=0`, `removidas=0` e as mesmas contagens do item 4.

## Segurança

- Não executa `DROP` em tabelas permanentes nem `TRUNCATE` de Bronze/Silver.
- Não guarda endereços ou textos livres.
- As tabelas temporárias desaparecem ao final da transação.
- Uma falha antes do `COMMIT` reverte a criação/atualização das associações.
- Não execute `docker compose down -v`.
