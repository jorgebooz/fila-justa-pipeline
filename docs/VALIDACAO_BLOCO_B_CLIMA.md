# Bloco B — Conector Open-Meteo (São Paulo)

## Posição no roteiro

Passo 4 (ingestão bronze), após a carga do SP156 e do IBGE. Este patch **não altera** os scripts que já funcionam no projeto, o Docker Compose ou o DDL.

## Fonte das coordenadas

O agregado populacional do IBGE não contém latitude e longitude. Por isso, antes de buscar a previsão, `include/ingest/gerar_pontos_clima.py` obtém a **malha de distritos do Censo 2022 (UF SP)** e gera um arquivo `include/seeds/pontos_clima_ibge_sp.csv` com um ponto geométrico para cada um dos 96 códigos oficiais de distrito de São Paulo. A leitura do SHP/DBF é feita só com a biblioteca padrão do Python. Para áreas em que o centroide cai fora do polígono, usa-se um ponto interno de referência, e o método é registrado no CSV.

Fonte: https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/malha_com_atributos/distritos/shp/UF/SP/SP_distritos_CD2022.zip

**limitação:** centroide plano em graus (SIRGAS 2000) é aproximação para *ponto de consulta meteorológica*, não para cálculo cartográfico de áreas. Apenas a previsão para um ponto representa cada distrito; há variação espacial dentro de distritos grandes.

## Preparação

Aplique **somente** os arquivos deste patch na pasta do projeto, mantendo os scripts SP156 e IBGE atuais e o `.env` local. Antes do clima, `bronze.raw_ibge_area` precisa ter 96 distritos.

No Git Bash, na raiz `fila-justa-data`:

```bash
python -m unittest discover -s tests -v

# 1) Gerar os pontos usando a malha oficial do IBGE:
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/ingest/gerar_pontos_clima.py

# Conferir a saída local: 96 dados + 1 cabeçalho
wc -l include/seeds/pontos_clima_ibge_sp.csv

# 2) Ingerir as próximas 48 horas de previsão para todos os distritos:
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/ingest/clima.py
```

Se o download da malha falhar por certificado ou rede, obtenha o ZIP pelo link oficial e use o argumento `--arquivo /caminho/do/arquivo.zip` **com o caminho visível dentro do container**. Não desabilitar verificação de certificado sem investigar.

## Validar no PostgreSQL

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT consultado_em, COUNT(*) AS linhas, COUNT(DISTINCT area_id) AS distritos
FROM bronze.raw_clima
GROUP BY consultado_em
ORDER BY consultado_em DESC
LIMIT 5;
"'
```

**Esperado para cada snapshot concluído:** 4.608 registros (96 × 48), 96 distritos. A quantidade só estará comprovada após a execução real no Docker.

### Idempotência

Uma segunda execução **dentro da mesma hora UTC** usa o mesmo `consultado_em` (início da hora) e não duplica linhas. Se a previsão mudar entre consultas, as linhas com conteúdo diferente são atualizadas (o contador `inseridas_ou_alteradas` pode ser maior que zero, mesmo sem duplicações). Uma execução em outra hora gera novo snapshot legítimo, preservando o histórico para auditoria.

### O que é e o que não é este clima

- A API retorna a **previsão atual para as próximas 48 horas**, e não a chuva que ocorreu entre janeiro e setembro de 2026.
- O parâmetro `forecast_hours=48` começa na hora atual; `hourly=precipitation` é dado em milímetros por hora, como soma da hora anterior. Usamos UTC para guardar os timestamps de forma inequívoca.
- Ainda precisamos transformar a bronze na `silver.clima_previsao` e consolidar `chuva_48h_mm` por distrito nas etapas posteriores.
- Não preenchemos uma previsão ausente com zero silenciosamente: a ingestão falha de forma explícita se faltarem horas ou valores.

Documentação oficial: https://open-meteo.com/en/docs | https://open-meteo.com/en/terms

## Observações sobre os testes

Os testes `tests/test_clima.py` são unitários com respostas e shapefile sintéticos. Eles não validam a malha real, o serviço remoto ou o PostgreSQL. A geração da seed e a ingestão real ainda precisam ser executadas no ambiente Docker do usuário para serem aceitas.
