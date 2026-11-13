# Fontes de dados

> **Estado observado em 12/11/2026:** fontes SP156, IBGE e Open-Meteo foram ingeridas com sucesso e validadas na Bronze; confira contagens e rodadas em `docs/evidencias/resultados_validacao.txt`. As pendências listadas abaixo são histórico do processo de exploração e não bloqueios da carga implementada. O 4º trimestre de 2026 ainda não estava publicado nas execuções registradas.

## 1. SP156 — solicitações de munícipes (fonte principal de ocorrências)

| Item | Situação |
|---|---|
| Dataset | https://dados.prefeitura.sp.gov.br/dataset/dados-do-sp156 (Portal de Dados Abertos da Prefeitura de São Paulo, CKAN) |
| Publicador | SMIT — Secretaria Municipal de Inovação e Tecnologia |
| Licença | **Creative Commons CCZero (cc-zero)**, conforme declarada pelo CKAN na execução de 19/10/2026 |
| Canais cobertos | Central telefônica, portal, aplicativo, Descomplica e praças de atendimento das subprefeituras (nota da página do dataset) |
| Recursos no dataset | 136 (CSV, XLSX, PDF de release, TXT de dicionário), dataset modificado em 2026-10-19 |
| Formato das solicitações | **1 CSV (e 1 XLSX) por trimestre**, de 1º TRI 2015 até **3º TRI 2026** na exploração anterior (verificar novamente disponibilidade ao executar) (CSV modificado em 2026-10-13). Para 2012–2014 os arquivos são semestrais e vêm do sistema antigo, com campos e nomenclatura diferentes |
| Tamanho | 3º TRI 2026: 105.625.875 bytes (~101 MB). Nos trimestres de 2022 a 2026 os CSV ficam entre ~85 MB e ~155 MB |
| Dicionário | Arquivo TXT "Dicionário dados abertos - SP156 2020 em diante" listado no dataset. Na página do recurso do 1º TRI 2024 o dicionário lista as colunas `Data de abertura` (timestamp), `Serviço` (text), `Subprefeitura` (text), `Latitude` (numeric) e `Longitude` (numeric) (referência histórica; a lista real do arquivo 3º TRI 2026 está abaixo) |
| Colunas reais do arquivo escolhido | 15: `Data de Abertura`, `Data de Finalização`, `Meio de Comunicação`, `Tema`, `Assunto`, `Serviço`, `Status`, `Órgão`, `Logradouro`, `Número`, `CEP`, `Latitude`, `Longitude`, `Prefeitura Operacional`, `Distrito` (relatório local) |
| Encoding e delimitador | `utf-8-sig`, vírgula (CSV 3º TRI 2026, execução local) |
| Linhas por trimestre | 3º TRI 2026: **484.232** linhas (execução local, arquivo inteiro) |
| Cobertura de latitude/longitude | **303.748 / 484.232 (62,7%)** têm ambas; **302.923 (62,6%)** com `Distrito` preenchido. A coluna `Distrito` contém nomes e códigos numéricos, portanto necessita normalização validada |
| Período escolhido | **Decisão confirmada:** ano civil **2026 inteiro (01/01 a 31/12/2026)**, reunindo os arquivos dos quatro trimestres. Em 11/11/2026 a exploração local havia identificado arquivos até o 3º TRI; o 4º TRI só poderá integrar o resultado quando for publicado. O relatório do 3º TRI encontrou datas no formato **MM/DD/AAAA**; conferir os demais trimestres antes da ingestão |
| Filtros (serviços de interesse) | **Seed preliminar criada:** `include/seeds/servicos_interesse.csv`, a partir do catálogo anual com 880 nomes distintos, selecionando 10 serviços e 5 tipos. Critérios e quantidades em `docs/validacao_catalogo_2026.md`. Confirmar semanticamente com glossário e validar cobertura por serviço antes da ingestão |

**Glossários:** "Glossário de Dados SP156 (Status das Solicitações) - 2012 até 2022" (XLSX, listado no dataset) e os dicionários TXT por período.

**Inconsistências encontradas (registradas porque afetam a ingestão):**

- O texto da página do dataset diz que o período vai até julho de 2023, mas os arquivos listados vão até o 3º TRI 2026. A nota da página está desatualizada; vale a lista de recursos.
- O rótulo de formato varia (`CSV`, `.csv`), e o recurso "2º TRI 2021" está rotulado como CSV mas a URL aponta para um `.xlsx`. A seleção automática do arquivo precisa ignorar esse caso.
- O mesmo dataset também contém a **pesquisa de satisfação dos canais** ("Avaliação dos canais", CSV com as colunas `Canal`, `Data da Pesquisa`, `Navegação`, `Informação`, `Visual`, `Encontrou o que precisava`, `Avaliação Geral`). Ela **não** é usada neste projeto.
- A execução precisou desligar a verificação SSL para o domínio da prefeitura. Por serem dados públicos sem credenciais, o risco é baixo; no pipeline isso deve ficar explícito e configurável.

**Limitações documentadas pela própria prefeitura (nota da página do dataset):**

- Os dados contam **solicitações**, não problemas: uma pessoa pode abrir várias solicitações para o mesmo problema e várias pessoas podem pedir o mesmo serviço. É a justificativa direta para a etapa de deduplicação.
- Serviços podem ser reclassificados ou encaminhados entre órgãos, e a nomenclatura muda entre versões.
- O status é um retrato da data da extração.
- Publicações novas podem ter pequenos acréscimos de protocolos que antes eram rascunho.

**Alternativa georreferenciada (GeoSampa):** o catálogo de metadados do GeoSampa descreve camadas de serviços SP156 georreferenciadas, limitadas às solicitações dos últimos 6 meses disponíveis no Portal de Dados Abertos. Só será usada se os CSV não trouxerem coordenadas utilizáveis.

## 2. IBGE — Censo 2022, agregados (base da equidade por distrito)

O IBGE publica oficialmente os **Agregados por Distritos — Resultados do universo** em CSV/XLSX, na página do Censo 2022. Arquivos base publicados no FTP oficial:

- Página de downloads: https://www.ibge.gov.br/estatisticas/sociais/populacao/22827-censo-demografico-2022.html?edicao=41852&t=downloads
- Diretório CSV por distrito: https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/Agregados_por_Distrito_csv/
- Nota metodológica e convenções: https://biblioteca.ibge.gov.br/visualizacao/livros/liv102136.pdf

**Confirmado em fonte oficial:** há agregados por distrito, arquivo básico e temas de domicílios/pessoas; valores `x` de sigilo **não podem virar zero**. **Atenção:** o CSV básico efetivamente recebido estava em **Latin-1**, não UTF-8, com separador `;`; respeitar a codificação detectada no arquivo real. A página lista arquivos atualizados em 2025/2026.

**Inspeção local concluída:** ZIP `Agregados_por_distritos_basico_BR_20260520.zip` (anexado), CSV Latin-1 separado por `;`, filtro `CD_MUN=3550308`: 96 distritos, `CD_DIST` (código), `NM_DIST` (nome), `v0001` (população), `v0002` (total de domicílios). Mapeamento nominal dos 96 distritos confirmado contra o relatório SP156 do 3º trimestre. Os códigos numéricos do SP156 permanecem sem equivalência validada. Detalhes em `docs/validacao_ibge_2022.md`. O DDL contém campos **normalizados**, não nomes de CSV.

**Critério de ingestão:** carregar apenas registros municipais de São Paulo (código IBGE 3550308) e as variáveis conferidas no dicionário; rejeitar/contabilizar valores `x`, `..` e `...` como ausentes, sem inventar valores. Para centroides, obter geometria pública de distritos e calcular centroides antes de chamar Open-Meteo por área.

## 3. Open-Meteo — previsão de chuva

| Item | Situação |
|---|---|
| Endpoint | `https://api.open-meteo.com/v1/forecast` |
| Parâmetros usados | `latitude`, `longitude`, `hourly=precipitation`, `forecast_days` |
| Várias coordenadas | **Confirmado em execução:** uma chamada com 2 pontos devolveu HTTP 200 e 2 resultados, cada um com 48 horas e unidade `mm` |
| Precipitação horária | Soma da hora anterior, em mm (documentação) |
| Horizonte | 7 dias por padrão; até 16 com `forecast_days` (documentação) |
| Resolução espacial | A API devolve as coordenadas do **ponto de grade**, não as pedidas. Na execução: pedido (-23.55, -46.63) → devolvido (-23.514938, -46.610504); pedido (-23.60, -46.65) → devolvido (-23.585238, -46.659637). Suficiente para uma previsão por distrito, e fica registrado como limitação |
| Chave de API | Não necessária para uso não comercial |
| Licença / atribuição | CC BY 4.0. Citar "Weather data by Open-Meteo.com" com link. Uso gratuito apenas não comercial; conferir os limites de chamadas nos termos oficiais (https://open-meteo.com/en/features#terms) |

## 4. Relatos do app (simulados)

Gerador próprio, documentado em `docs/simulacao.md` (passo 5).

## Registro histórico de pendências da exploração (não bloqueiam a carga já executada)

1. **Resolvido para validação:** arquivo básico IBGE recebido e colunas reais confirmadas (`CD_DIST`, `NM_DIST`, `v0001`, `v0002`). Ainda falta programar a obtenção reprodutível durante a ingestão.
2. Validar nomes exatos de serviços do SP156 para construir `servicos_interesse.csv`, evitando filtrar ocorrências indevidas.
3. **Parcialmente resolvido:** 96 nomes distintos do SP156 coincidem com 96 nomes do IBGE; 94 valores numéricos do SP156 continuam sem mapeamento confirmado. Não realizar junção numérica por suposição.
4. Documentar a escolha entre (a) manter apenas ocorrências com localização e distrito identificável ou (b) enriquecer coordenadas com malha territorial, medindo descarte.
5. Testar a disponibilidade da previsão no momento da execução; não usar previsão de novembro como se fosse a do trimestre histórico.

## Recorte anual confirmado — 2026

- Para explorar todos os trimestres disponíveis de 2026: `python scripts/explorar_fontes.py --ano 2026`. O script consulta o catálogo CKAN, processa cada CSV trimestral encontrado e gera `docs/catalogo_servicos_sp156.csv` com frequências agregadas do período consultado.
- Se algum trimestre não estiver publicado, o relatório avisa que **o ano está incompleto**. Não interpretar ausência como zero e não atribuir cobertura anual aos totais parciais.
- O conjunto final de 2026 será 1T + 2T + 3T + 4T após a publicação do último trimestre; a ingestão futura deve ser incremental/idempotente por fonte/período para permitir recarga do 4T sem duplicação.
- O catálogo agregado não prova que o dado já foi carregado na bronze: exploração e ingestão são etapas distintas.
- A previsão atual do Open-Meteo não representa condições climáticas passadas de janeiro a setembro de 2026: o score climático deve ser associado à **rodada atual de priorização** e a ocorrências ainda abertas, não reinterpretado como previsão histórica.
