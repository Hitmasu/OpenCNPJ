<img src="./src/Page/assets/logo.svg" alt="OpenCNPJ" height="64" />

Projeto aberto para baixar, processar e publicar dados públicos das empresas do Brasil.

## Pastas

- `src/ETL/Processor`: ETL que baixa, processa e publica dados do CNPJ.
- `src/ETL/Tests`: testes do ETL.
- `src/ETL/OpenCNPJ.sln`: solution do ETL.
- `src/Page`: página/SPA estática para consulta dos dados publicados.
- `src/Worker`: Worker Cloudflare que lê shards publicados no R2.
- `src/script`: scripts operacionais, incluindo o deploy versionado do Worker.

## Requisitos

- `.NET SDK 10.0+`
- `rclone` instalado e autenticado no seu storage (ex.: Backblaze, R2, S3, Azure Storage, ...).
- `bq` instalado e autenticado somente quando `BigQuery.Enabled=true`.
- Espaço em disco e boa conexão (a primeira execução pode levar tempo -- dias até).

## Configuração

- Ajuste `src/ETL/Processor/config.json` se desejar mudar pastas locais, destino do storage, memória, paralelismo...
- No `config.json`, aponte para o Storage que deseja passando a configuração do rclone.
- O downloader da Receita agora usa WebDAV no share público do SERPRO+/Nextcloud.
- A integração do Portal da Transparência habilita 12 módulos independentes. Os módulos históricos consideram somente períodos de 2013 em diante; quando a fonte começa depois desse corte, a publicação começa no primeiro período realmente disponível.
- `PortalTransparenciaIntegration.ProcessingPartitions` controla em quantas partições de CNPJ os CSVs grandes são projetados. O padrão é 64, com `DuckDbMemoryLimit=512MB` e spill temporário limitado; a exportação dos shards também opera em streaming sob esse limite. Os datasets particionados ainda subdividem os registros de cada CNPJ em 32 grupos limitados antes da agregação, evitando um único JSON intermediário gigante. Anos formados por arquivos mensais são projetados um mês por vez.
- A publicação no BigQuery fica desligada por padrão. Com `BigQuery.Enabled=false` e sem override, o pipeline apenas emite um aviso e segue sem exigir `bq` ou credenciais Google.
- Para habilitar BigQuery, configure `BigQuery.Enabled=true` ou `OPENCNPJ_BIGQUERY_ENABLED=true`, além de `Dataset`, `TablePrefix` opcional, `Location` opcional e `BqExecutable` se o binário não for `bq`. O projeto pode vir de `BigQuery.ProjectId` no `config.json` ou da env `OPENCNPJ_BIGQUERY_PROJECT_ID`; as envs têm prioridade. Em container, `OPENCNPJ_GOOGLE_CREDENTIALS_BASE64` pode receber a credencial Google codificada em base64 como secret do ambiente.

## Layout local

- `downloads/YYYY-MM`: zips baixados da Receita.
- `extracted_data/YYYY-MM`: arquivos extraídos para o mês.
- `parquet_data/YYYY-MM`: Parquets gerados para o mês e Parquets mais recentes das integrações.
- `parquet_data/YYYY-MM/bigquery/receita/part-*.parquet`: Parquets colunares da Receita para BigQuery, com 1 linha por CNPJ.
- `parquet_data/YYYY-MM/integrations/{cno,rntrc}/bigquery/*.parquet`: Parquets colunares das integrações para BigQuery.
- `parquet_data/integrations/{dataset}/segments/{YYYY|YYYY-MM}/{source_version}/part-*.parquet`: partes canônicas dos módulos históricos do Portal da Transparência.
- `cnpj_shards/YYYY-MM/releases/{release_id}/shards`: shards locais `*.ndjson` e `*.index.bin` do release atual.

Os artefatos locais não são apagados automaticamente, exceto quando o pipeline é executado com `--cleanup-on-success`. Nesse modo, o cleanup remove downloads, CSVs extraídos e temporários, mas preserva Parquets e releases locais para permitir recomposição incremental.

## Integrações

- O ETL possui a interface interna `IDataIntegration` para sub-módulos de dados.
- Cada integração declara chave, propriedade JSON, frequência de atualização e versão de schema.
- O estado de cada integração é publicado via rclone em `files/integrations/state/{module}/hashes.json`; módulos segmentados também persistem a lista e a versão dos segmentos locais.
- Integrações geram um ou mais Parquets canônicos com as colunas `cnpj`, `cnpj_prefix`, `payload_json`, `content_hash` e datas de origem/módulo. Datasets grandes podem produzir vários blocos contíguos para o mesmo CNPJ; o índice binário coalesce esses blocos em uma única faixa e o Worker recompõe a coleção.
- Quando publicadas no BigQuery, as integrações com suporte dedicado geram um Parquet columnar separado, sem `payload_json`.
- O JSON final sempre inclui a chave das integrações habilitadas; quando o CNPJ não tiver dado naquela integração, o valor fica `null`.

### Portal da Transparência

Cada conjunto oficial é tratado como um dataset independente:

- Fotografias atuais: `favorecidos_pj`, `ceis`, `cepim`, `cnep`, `acordos_leniencia`, `convenios` e `emendas_parlamentares`.
- Históricos segmentados: `licitacoes`, `contratos`, `renuncias_fiscais`, `notas_fiscais` e `emendas_documentos`.

Nos históricos mensais, anos encerrados são consolidados em um segmento `YYYY`; o ano corrente usa segmentos `YYYY-MM`. Bases anuais usam `YYYY`. Na virada do ano, o segmento anual substitui os segmentos mensais daquele exercício. A API consulta um índice binário de roteamento por CNPJ e busca somente os segmentos nos quais aquele CNPJ aparece, reunindo as coleções na resposta.

Disponibilidade efetiva após o corte de 2013:

- Licitações e contratos: desde 2013.
- Renúncias fiscais: desde 2015.
- Notas fiscais: desde novembro de 2019.
- Documentos de emendas: desde 2014.

O ZIP oficial de licitações de dezembro de 2018 (`201812_Licitacoes.zip`) está truncado na origem em 8 MiB. A esteira exclui esse artefato conhecido, emite um aviso explícito e mantém dezembro de 2018 documentado como lacuna; os demais meses de 2018 continuam no segmento anual.

## Execução

- Dentro de `src/ETL/Processor`:
  - `dotnet run pipeline`
  - `dotnet run pipeline -m YYYY-MM` (opcional)
  - `dotnet run pipeline --release-id abc123...` (opcional, força o release id remoto)
  - `dotnet run pipeline --cleanup-on-success` (opcional, remove artefatos locais do dataset após sucesso)

Sem `-m`, o pipeline escolhe o mês mais recente publicado no share WebDAV da Receita.

Quando `BigQuery.Enabled=true`, o pipeline carrega 1 tabela por módulo: `receita`, `cno`, `rntrc` e futuras integrações que publiquem um Parquet canônico. `receita`, `cno` e `rntrc` são carregadas a partir de Parquets colunares, sem `payload_json`. A carga acontece após shards/ZIPs serem gerados e antes do `info.json` e do estado de integração serem publicados, para falhar o release antes de marcá-lo como concluído.

Antes de carregar tabelas, o deploy e o ETL executam uma validação via `bq show {ProjectId}:{Dataset}`. O BigQuery pode ser ligado por `OPENCNPJ_BIGQUERY_ENABLED=true`, e o `ProjectId` vem de `OPENCNPJ_BIGQUERY_PROJECT_ID` ou do `config.json`. O projeto é passado explicitamente em todos os comandos BigQuery, então o fluxo não depende do projeto default configurado no `gcloud`. O dataset precisa existir; as tabelas finais podem existir ou não. Cada execução carrega uma tabela staging a partir do Parquet columnar e substitui/cria a tabela final com `bq cp --force`, permitindo atualizar schemas desatualizados, como a tabela `receita`, e criar tabelas novas, como `cno` e `rntrc`.

Para deploy em container com credencial via env, gere o valor com `base64 -w 0 arquivo.json` e configure `OPENCNPJ_GOOGLE_CREDENTIALS_BASE64` como secret. O entrypoint decodifica a credencial em arquivo temporário, ativa `gcloud auth activate-service-account` e remove o arquivo antes de iniciar o deploy.

### Página estática

- Dentro de `src/Page`:
  - `npm install`
  - `npm run dev` para desenvolvimento local
  - `npm run build` para gerar a versão estática em `src/Page/dist`

A página é implementada em React + TypeScript e continua sendo 100% estática no artefato final. O `index.html` de publicação deve ser o gerado em `src/Page/dist`, não o arquivo fonte usado pelo Vite em `src/Page/index.html`.

## Publicação

- Os shards no R2 são publicados em releases versionados, por exemplo `files/shards/releases/{release_id}/{prefix}.ndjson`.
- Cada dataset publicado também gera um ZIP estável por dataset em `files/releases/{dataset}/data.zip` (ex.: `receita`, `cno`, `rntrc`), referenciado no `info.json` com tamanho e MD5.
- Módulos históricos publicam ZIPs imutáveis por segmento em `files/releases/{dataset}/segments/{segmento}/data.zip`, shards em `files/shards/modules/{dataset}/segments/{segmento}/{release_id}` e roteamento em `files/shards/modules/{dataset}/routing/{release_id}`. Uma atualização mensal envia apenas o novo segmento e o novo índice de roteamento completo.
- A API publicada consome os `*.ndjson` e `*.index.bin` do release ativo no R2, onde `prefix` usa os 3 primeiros caracteres do CNPJ normalizado.
- Cada linha do `*.ndjson` representa um CNPJ e o `*.index.bin` guarda `offset` e `length` exatos para leitura direta no Worker.
- O contrato de CNPJ já aceita formato alfanumérico: 12 caracteres alfanuméricos + 2 dígitos finais.
- O arquivo `info.json` continua sendo publicado com metadados do release ativo, `storage_release_id`, ZIP principal da Receita e o mapa `datasets` com metadados de ZIP por base.

## Deploy

- Use `src/script/deploy.sh` para orquestrar o release:
  - roda o ETL com release versionado
  - ignora BigQuery com aviso quando `BigQuery.Enabled=false`
  - valida `OPENCNPJ_BIGQUERY_ENABLED` ou `BigQuery.Enabled`, `OPENCNPJ_BIGQUERY_PROJECT_ID` ou `BigQuery.ProjectId`, `BigQuery.Dataset`, o comando `bq` configurado e o acesso ao dataset quando BigQuery está habilitado
  - copia `info.json` e `*.index.bin` do release gerado para `src/Worker/assets`
  - executa `npm test` no Worker
  - faz `npx wrangler deploy`
  - valida `/info`, um CNPJ canônico e o mesmo CNPJ mascarado
  - remove o release antigo do bucket só depois da validação

## Contribuição

- Abra issues para discutir mudanças.
- Faça fork, crie uma branch descritiva e envie PR.
- Mantenha commits pequenos e o projeto compilando (`dotnet build`).
