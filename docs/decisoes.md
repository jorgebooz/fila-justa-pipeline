# Decisões de arquitetura

## Escopo e visão

O MVP começa na **cidade de São Paulo**, mas a ideia do Fila Justa é **expandir para todo o Brasil**.
O pipeline desta entrega usa dados de SP; o desenho (camadas, contrato de saída, parâmetros em
configuração) foi pensado para que novas cidades entrem sem refazer a arquitetura.

> limitação: a entrega cobre só São Paulo (SP156, distritos de SP). Evolução: um adaptador de fonte por
> cidade (o `municipio_cod` já entra no modelo, ver D5).

## Decisões confirmadas

| # | Decisão | Escolha | Opção descartada | Justificativa |
|---|---|---|---|---|
| D1 | Onde roda o Airflow | Local, via docker-compose | Hospedar na nuvem | Gera as evidências com baixo custo e esforço |
| D2 | Warehouse | PostgreSQL | DuckDB / Parquet | O MVP consome por SQL; trocar por Postgres gerenciado é só mudar a string de conexão |
| D3 | Granularidade da área (equidade) | Distrito | Setor censitário | Agregados do IBGE por distrito existem e o join é simples |
| D4 | Fonte principal de ocorrências | SP156 (Prefeitura de SP) | Outra cidade | Dados abertos confirmados |

## Decisões adicionais confirmadas

| # | Decisão | Escolha | Justificativa |
|---|---|---|---|
| D5 | Modelo preparado para várias cidades | Coluna `municipio_cod` (código IBGE do município) nas tabelas silver e gold; parâmetros por município em configuração | Sustenta a expansão de São Paulo para o Brasil sem refazer a arquitetura |
| D6 | Versão do Airflow | Versão 3.4.0 fixada no compose desta entrega; verificar versões disponíveis antes de atualizar | Garante reprodutibilidade dos testes apresentados; atualizações exigem regressão |

## Justificativa de ELT

O ELT preserva os campos operacionais necessários na Bronze, executa as transformações versionadas dentro do PostgreSQL e mantém identificadores de rodada para auditoria. Os CSVs completos não são guardados na Bronze por minimização de dados (LGPD). H3 e geração sintética são passos Python explicitados como complementos ao ELT.


## ELT implementado e decisões operacionais finais (Passos 4-13)

- **ELT no warehouse:** o Python consulta as fontes e carrega projeções sanitizadas na Bronze; a padronização, a deduplicação, a equidade e a pontuação usam SQL executado no PostgreSQL. A biblioteca H3 em Python é usada apenas para indexação espacial (operações que não estão disponíveis nativamente na instalação do Postgres). A simulação é uma etapa geradora em Python, explicitamente identificada como sintética.
- **Rodadas e publicação:** os scores e a equidade possuem `run_id` por rodada; o fato `fact_ocorrencia` é o estado atual. As duas views Gold usam `fact_ocorrencia.run_id = fact_score.run_id` para publicar uma única rodada coerente. Correção documentada em `docs/CORRECAO_PUBLICACAO_MARTS.md`.
- **Localização em clusters transdistritais:** o distrito Gold é o do relato mais antigo (empate pelo ID). A verificação em produção observou 115 clusters transdistritais. A equidade usa contagem dos relatos elegíveis Silver antes de deduplicar.
- **H3:** resolução 9, janela de sete dias ancorada no primeiro relato. Pontos de células vizinhas não são fundidos automaticamente.
- **Equidade:** `necessidade=1` é hipótese inicial. Fator somente reforça a componente exposição, no intervalo [1, 2].
- **Clima:** previsão de 48 horas do snapshot atual; não é medida histórica do dia do problema urbano.
- **Sensibilidade do score:** o arquivo `docs/analise_sensibilidade.sql` compara cenários de `alpha` e teto `M` para a mesma rodada publicada, sem atualizar fatos. A execução quantitativa desse SQL ainda está pendente; **não** há resultado numérico de sensibilidade documentado ou inventado.
- **Reprodutibilidade:** o conjunto de 116 testes Python e 37 regras SQL foi aprovado; DAG com 13 tarefas verdes. A execução completa em banco **inteiramente vazio** ainda não foi demonstrada com captura real.
