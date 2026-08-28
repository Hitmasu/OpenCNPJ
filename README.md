<img src="./src/Page/assets/logo.svg" alt="OpenCNPJ" height="64" />

Projeto aberto para baixar, processar e publicar dados públicos das empresas do Brasil.

## Pastas

- `src/ETL/Processor`: ETL que baixa, processa e publica dados do CNPJ.
- `src/ETL/Tests`: testes do ETL.
- `src/ETL/OpenCNPJ.sln`: solution do ETL.
- `src/Page`: página/SPA estática para consulta dos dados publicados.
- `src/Worker`: Worker Cloudflare que lê shards publicados no R2.
- `src/scripts`: scripts operacionais, incluindo o deploy versionado do Worker.

## Requisitos

- `.NET SDK 10.0+`
- `rclone` instalado e autenticado no seu storage (ex.: Backblaze, R2, S3, Azure Storage, ...). Por padrão o comando é `rclone`, mas `Rclone.Executable` pode apontar para um caminho absoluto.
- `bq` instalado e autenticado somente quando `BigQuery.Enabled=true`.
- Espaço em disco e boa conexão (a primeira execução pode levar tempo -- dias até).

## Configuração

O arquivo principal de configuração do ETL é `src/ETL/Processor/config.json`. Os valores abaixo refletem o arquivo versionado atual; caminhos relativos são resolvidos a partir do diretório em que o ETL roda, normalmente `src/ETL/Processor`.

### Paths

| Campo | Tipo | Padrão no `config.json` | Descrição |
|---|---|---|---|
| `DataDir` | string | `./extracted_data` | Diretório dos CSVs extraídos e dos insumos das integrações. Também guarda o estado local das integrações em `integrations/_state`. |
| `ParquetDir` | string | `./parquet_data` | Diretório dos Parquets gerados pelo ETL e pelas integrações. É preservado pelo cleanup para permitir recomposição incremental. |
| `OutputDir` | string | `./cnpj_shards` | Diretório dos shards, índices, ZIPs e metadados de release antes da publicação. |
| `DownloadDir` | string | `./downloads` | Diretório dos ZIPs baixados da Receita Federal. |

### Rclone

| Campo | Tipo | Padrão no `config.json` | Descrição |
|---|---|---|---|
| `Executable` | string | `rclone` | Nome ou caminho do executável do rclone. Se ausente, vazio ou só com espaços, o ETL e os scripts usam `rclone`. |
| `RemoteBase` | string | `Opencnpj:opencnpj/files` | Remote e diretório base de publicação. Pode ser sobrescrito por `RCLONE_REMOTE`. |
| `Transfers` | int | `4` | Número de transferências paralelas passadas para `rclone copy`. O ETL usa no mínimo `1`. |
| `MaxConcurrentUploads` | int | `1` | Limite de uploads simultâneos coordenados pelo ETL. |
| `BufferSize` | string | `16M` | Valor de `--buffer-size` usado no rclone. Se ausente ou vazio, o ETL usa `16M`. |
| `UploadVerificationRetries` | int | `3` | Número de tentativas de upload com verificação de contagem/hash remoto. O ETL usa no mínimo `1`. |
| `UploadVerificationDelaySeconds` | int | `15` | Espera, em segundos, entre tentativas de verificação/upload. O ETL usa no mínimo `1`. |

`Rclone.Executable` escolhe o binário do rclone. `Rclone.RemoteBase` escolhe o remote e o destino dentro do storage. São configurações diferentes.

Quando o rclone está no `PATH`, mantenha:

```json
"Executable": "rclone"
```

Quando ele está fora do `PATH`, configure um caminho absoluto:

```json
"Executable": "/opt/rclone/rclone"
```

```json
"Executable": "C:\\Tools\\rclone\\rclone.exe"
```

Com um caminho absoluto, o fluxo que usa esse `config.json` não precisa descobrir o executável pelo `PATH`. A imagem Docker continua instalando `/usr/local/bin/rclone` e expondo `rclone` no `PATH`, então a configuração padrão segue funcionando em container.

### DuckDb

| Campo | Tipo | Padrão no `config.json` | Descrição |
|---|---|---|---|
| `UseInMemory` | bool | `false` | Usa DuckDB em memória (`true`) ou arquivo local `cnpj.duckdb` (`false`). |
| `ThreadsPragma` | int | `1` | Valor aplicado em `PRAGMA threads`; o ETL usa no mínimo `1`. |
| `MemoryLimit` | string | `4GB` | Limite de memória do DuckDB para o processamento principal. |
| `EngineThreads` | int | `1` | Valor aplicado em `SET threads`; o ETL usa no mínimo `1`. |
| `PreserveInsertionOrder` | bool | `false` | Controla `preserve_insertion_order` no DuckDB. `false` reduz pressão de memória. |
| `PartitionedWriteMaxOpenFiles` | int | `16` | Limite de arquivos abertos em escrita particionada; o ETL usa no mínimo `1`. |

### Shards

| Campo | Tipo | Padrão no `config.json` | Descrição |
|---|---|---|---|
| `PrefixLength` | int | `3` | Quantidade de caracteres do CNPJ usada como prefixo de shard e roteamento. |
| `RemoteDir` | string | `shards` | Subdiretório local/remoto onde os shards são organizados. |
| `MaxParallelProcessing` | int | `1` | Orçamento de paralelismo para geração de shards. O ETL usa no mínimo `1`. |
| `QueryBatchSize` | int | `1` | Quantidade de prefixos processados por lote de consulta. O ETL usa no mínimo `1`; se a propriedade faltar, o inicializador do código é `4`. |
| `QueryRangeFanOut` | int | `5` | Fator de subdivisão de faixas quando uma consulta de shard estoura memória. O ETL usa no mínimo `2`. |
| `QsaMaterializationRangeFanOut` | int | `2` | Fator de subdivisão usado na materialização de QSA durante conversão para Parquet. |

### Downloader

| Campo | Tipo | Padrão no `config.json` | Descrição |
|---|---|---|---|
| `ParallelDownloads` | int | `2` | Downloads simultâneos dos ZIPs da Receita. O downloader usa no mínimo `1`. |
| `PublicShareRoot` | string | URL WebDAV da Receita | Raiz pública WebDAV do SERPRO+/Nextcloud usada para listar meses e baixar ZIPs da Receita. |

### CnoIntegration

| Campo | Tipo | Padrão no `config.json` | Descrição |
|---|---|---|---|
| `Enabled` | bool | `true` | Habilita a integração do Cadastro Nacional de Obras. |
| `PublicShareRoot` | string | URL WebDAV do CNO | Pasta pública onde o ZIP do CNO é localizado. |
| `ZipFileName` | string | `cno.zip` | Nome exato do ZIP esperado na pasta pública. |
| `RefreshHours` | int | `24` | Intervalo lógico de atualização usado pela integração para versionar/reutilizar fonte. |

### RntrcIntegration

| Campo | Tipo | Padrão no `config.json` | Descrição |
|---|---|---|---|
| `Enabled` | bool | `true` | Habilita a integração do RNTRC. |
| `PackageShowUrl` | string | URL CKAN da ANTT | Endpoint `package_show` usado para descobrir o CSV mais recente. |
| `RefreshHours` | int | `24` | Intervalo lógico de atualização usado pela integração para versionar/reutilizar fonte. |

### PortalTransparenciaIntegration

| Campo | Tipo | Padrão no `config.json` | Descrição |
|---|---|---|---|
| `Enabled` | bool | `true` | Habilita as integrações do Portal da Transparência. |
| `CatalogBaseUrl` | string | `https://portaldatransparencia.gov.br/download-de-dados` | URL base dos catálogos de download. Deve ser HTTP(S) absoluta. |
| `EnabledDatasets` | string[] | 12 datasets | Lista de datasets habilitados. Vazio ou ausente habilita todos os datasets conhecidos. |
| `DuckDbThreads` | int | `1` | Threads usadas no processamento DuckDB específico do Portal. |
| `DuckDbMemoryLimit` | string | `512MB` | Limite de memória do DuckDB para módulos do Portal. |
| `DuckDbMaxTempDirectorySize` | string | `20GB` | Limite de spill temporário do DuckDB para módulos do Portal. |
| `ProcessingPartitions` | int | `64` | Número de partições de CNPJ usadas para projetar CSVs grandes. |

Datasets aceitos em `EnabledDatasets`: `favorecidos_pj`, `ceis`, `cepim`, `cnep`, `acordos_leniencia`, `licitacoes`, `contratos`, `renuncias_fiscais`, `notas_fiscais`, `convenios`, `emendas_parlamentares` e `emendas_documentos`. Também são aceitos os slugs dos catálogos correspondentes.

A integração do Portal habilita 12 módulos independentes. Os módulos históricos consideram somente períodos de 2013 em diante; quando a fonte começa depois desse corte, a publicação começa no primeiro período realmente disponível. Anos formados por arquivos mensais são projetados um mês por vez, e anos encerrados são consolidados em segmentos anuais.

### BigQuery

| Campo | Tipo | Padrão no `config.json` | Descrição |
|---|---|---|---|
| `Enabled` | bool | `false` | Habilita publicação no BigQuery. Com `false`, o pipeline emite aviso e segue sem exigir `bq` ou credenciais Google. Pode ser sobrescrito por `OPENCNPJ_BIGQUERY_ENABLED`. |
| `ProjectId` | string | vazio | Projeto BigQuery. Obrigatório quando BigQuery está habilitado; pode ser sobrescrito por `OPENCNPJ_BIGQUERY_PROJECT_ID`. |
| `Dataset` | string | `public` | Dataset BigQuery de destino. Deve ser um identificador ASCII simples e existir antes da publicação. |
| `TablePrefix` | string | vazio | Prefixo opcional para tabelas finais/staging. Se informado, deve manter nomes ASCII simples. |
| `Location` | string | vazio | Localização opcional passada como `--location` nos comandos `bq`. |
| `BqExecutable` | string | `bq` | Nome do comando `bq`. Se ausente ou vazio, o ETL usa `bq`. |
| `KeepStagingTables` | bool | `false` | Mantém tabelas staging após a cópia para a tabela final. Útil para depuração. |
| `CompactionThreads` | int | `1` | Threads usadas na compactação de Parquets de integrações genéricas para BigQuery. Deve ser maior que zero. |
| `CompactionMemoryLimit` | string | `4GB` | Limite de memória do DuckDB na compactação BigQuery. Obrigatório quando a compactação roda. |
| `CompactionMaxTempDirectorySize` | string | `100GB` | Limite de diretório temporário do DuckDB na compactação BigQuery. Obrigatório quando a compactação roda. |

Quando `BigQuery.Enabled=true`, o pipeline carrega 1 tabela por módulo: `receita`, `cno`, `rntrc` e futuras integrações que publiquem um Parquet canônico. O projeto é passado explicitamente em todos os comandos BigQuery, então o fluxo não depende do projeto default configurado no `gcloud`.

### Variáveis de ambiente

| Variável | Onde é usada | Precedência e efeito |
|---|---|---|
| `RCLONE_REMOTE` | ETL, deploy e entrypoint Docker | Quando definida, sobrescreve `Rclone.RemoteBase`. |
| `RCLONE_CONFIG_BASE64` | entrypoint Docker | Tem precedência sobre `RCLONE_CONFIG`; é decodificada para um arquivo temporário e exportada como `RCLONE_CONFIG`. |
| `RCLONE_CONFIG` | entrypoint Docker e rclone | Caminho de arquivo de configuração do rclone. No entrypoint, é obrigatório quando `RCLONE_CONFIG_BASE64` não foi definido. |
| `OPENCNPJ_BIGQUERY_ENABLED` | ETL, deploy e entrypoint Docker | Sobrescreve `BigQuery.Enabled`. Aceita apenas `true` ou `false`. |
| `OPENCNPJ_BIGQUERY_PROJECT_ID` | ETL, deploy e entrypoint Docker | Sobrescreve `BigQuery.ProjectId`; espaços nas pontas são removidos pelo ETL. |
| `OPENCNPJ_GOOGLE_CREDENTIALS_BASE64` | entrypoint Docker | Quando BigQuery está habilitado, pode conter a credencial Google em base64. O entrypoint decodifica, ativa `gcloud auth activate-service-account` e remove o arquivo temporário. |
| `OPENCNPJ_RELEASE_ID` | deploy | Define o release id usado pelo deploy, equivalente a passar `--release-id`. |
| `OPENCNPJ_BASE_URL` | deploy | URL pública usada na validação pós-deploy quando `--base-url` não é informado. |
| `OPENCNPJ_VALIDATE_CNPJ` | deploy | CNPJ usado nas validações pós-deploy. |
| `OPENCNPJ_FETCH_JSON_RETRIES` / `OPENCNPJ_FETCH_JSON_RETRY_DELAY_SECONDS` | deploy | Ajustam tentativas e espera ao buscar JSON de validação. |
| `OPENCNPJ_VALIDATE_RETRIES` / `OPENCNPJ_VALIDATE_RETRY_DELAY_SECONDS` | deploy | Ajustam tentativas e espera da validação semântica pós-deploy. |
| `OPENCNPJ_CHECK_INTERVAL_SECONDS` | entrypoint Docker | Intervalo do loop de deploy em container. Deve ser inteiro positivo; padrão `3600`. |

Não há variável de ambiente específica para `Rclone.Executable`; configure o executável no `config.json`.

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

- Use `src/scripts/deploy.sh` para orquestrar o release:
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
