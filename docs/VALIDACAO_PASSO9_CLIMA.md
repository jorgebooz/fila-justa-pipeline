# Passo 9 — Previsão climática na Silver

## Estado anterior confirmado

- Bronze SP156: 1.525.907 registros (1T, 2T, 3T/2026); segunda execução sem alterações.
- Bronze IBGE: 96 distritos; segunda execução sem alterações.
- Bronze Open-Meteo: snapshot confirmado no PostgreSQL com 4.608 registros e 96 distritos.
- Bronze app simulado: 889 relatos; segunda execução sem alterações.
- Silver ocorrências: 16.553; zero chaves nulas; segunda execução sem alterações.
- Clusters: 16.553 vínculos, 11.163 clusters. **115 clusters incluem relatos de diferentes distritos**.
- Equidade Gold: 96 distritos, 16.553 relatos elegíveis, 48 áreas reforçadas; segunda execução sem alterações.

## Instalação

Extraia somente os novos arquivos do patch para o projeto; não execute DDL novamente nem substitua os conectores existentes. A tabela `silver.clima_previsao` já existe.

Execute no Git Bash, na raiz do projeto:

```bash
python -m unittest discover -s tests -p 'test_clima_silver.py' -v
```

O clima representa as **próximas 48 horas a partir da consulta**, não a meteorologia passada das ocorrências de janeiro a setembro de 2026. Para pontuação operacional, atualize a Bronze climática antes da Silver:

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/ingest/clima.py
```

Execute o dry-run da transformação:

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/clima_silver.py --dry-run
```

Se obtiver **4.608 registros, 96 distritos e nenhuma falha**, carregue:

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/clima_silver.py --run-id clima_silver_2026
```

Confirme no PostgreSQL:

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT consultado_em, COUNT(*) AS registros, COUNT(DISTINCT area_id) AS distritos
FROM silver.clima_previsao GROUP BY consultado_em ORDER BY consultado_em DESC LIMIT 3;
SELECT COUNT(*) AS distritos, MIN(horas) AS min_horas, MAX(horas) AS max_horas,
       MIN(chuva_48h_mm) AS min_mm, MAX(chuva_48h_mm) AS max_mm
FROM silver.v_chuva_48h_area;
"'
```

O último snapshot da Silver deverá ter 4.608 linhas, 96 distritos e 48 horas por distrito; as somas de chuva dependem da previsão real. Não esperar valores fixos de chuva.

**Idempotência:** execute novamente `clima_silver.py --run-id clima_silver_2026`, sem atualizar a Bronze entre as duas execuções. O resultado esperado é `inseridas_ou_alteradas=0`. Um snapshot novo na Bronze deve gerar mais 4.608 registros na Silver sem apagar o anterior.

## Distrito para clusters que atravessam limites

Os **115 clusters com mais de um distrito não são um erro do H3**: a célula pode cruzar a fronteira distrital. A proposta para `gold.fact_ocorrencia` (Passo 10) é selecionar o `area_id` do **primeiro relato**, ordenado por `aberta_em, ocorrencia_id`. A atribuição deve ser determinística; não distribuir o cluster entre dois distritos nem descartar relatos. A equidade do Passo 8 usa os relatos individuais da Silver por distrito e **não precisa ser recalculada** por causa disso.

### Consulta de conferência da regra futura

```sql
WITH primeiro AS (
  SELECT c.cluster_id, o.area_id, o.ocorrencia_id,
         ROW_NUMBER() OVER (
           PARTITION BY c.cluster_id ORDER BY o.aberta_em, o.ocorrencia_id
         ) AS posicao
  FROM silver.cluster_ocorrencia c
  JOIN silver.ocorrencia_limpa o USING (ocorrencia_id)
)
SELECT COUNT(*) AS clusters_com_area_atribuivel
FROM primeiro WHERE posicao = 1;
```

**Limitações:** a precipitação horária do Open-Meteo representa o total da hora anterior, e `forecast_hours=48` inicia na hora atual; logo o uso das 48 marcas horárias é uma aproximação operacional da janela futura. A previsão é por ponto representativo do distrito; não captura microclimas. Quando não houver previsão válida, não presumir chuva zero sem sinalizar isso no score.
