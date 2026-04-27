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
- Espaço em disco e boa conexão (a primeira execução pode levar tempo -- dias até).

## Configuração

- Ajuste `src/ETL/Processor/config.json` se desejar mudar pastas locais, destino do storage, memória, paralelismo...
- No `config.json`, aponte para o Storage que deseja passando a configuração do rclone.
- O downloader da Receita agora usa WebDAV no share público do SERPRO+/Nextcloud.

## Layout local

- `downloads/YYYY-MM`: zips baixados da Receita.
- `extracted_data/YYYY-MM`: arquivos extraídos para o mês.
- `parquet_data/YYYY-MM`: Parquets gerados para o mês e Parquets mais recentes das integrações.
- `cnpj_shards/YYYY-MM/releases/{release_id}/shards`: shards locais `*.ndjson` e `*.index.bin` do release atual.

Os artefatos locais não são apagados automaticamente, exceto quando o pipeline é executado com `--cleanup-on-success`. Nesse modo, o cleanup remove downloads, CSVs extraídos e temporários, mas preserva Parquets e releases locais para permitir recomposição incremental.

## Integrações

- O ETL possui a interface interna `IDataIntegration` para sub-módulos de dados.
- Cada integração declara chave, propriedade JSON, frequência de atualização e versão de schema.
- O estado de hashes por CNPJ de cada integração é publicado via rclone em `files/integrations/state/{module}/hashes.json`.
- Integrações devem gerar Parquet com 1 linha por CNPJ (`cnpj`, `cnpj_prefix`, `payload_json`, `content_hash`, datas de origem/módulo).
- O JSON final sempre inclui a chave das integrações habilitadas; quando o CNPJ não tiver dado naquela integração, o valor fica `null`.

## Execução

- Dentro de `src/ETL/Processor`:
  - `dotnet run pipeline`
  - `dotnet run pipeline -m YYYY-MM` (opcional)
  - `dotnet run pipeline --release-id abc123...` (opcional, força o release id remoto)
  - `dotnet run pipeline --cleanup-on-success` (opcional, remove artefatos locais do dataset após sucesso)

Sem `-m`, o pipeline escolhe o mês mais recente publicado no share WebDAV da Receita.

### Página estática

- Dentro de `src/Page`:
  - `npm install`
  - `npm run dev` para desenvolvimento local
  - `npm run build` para gerar a versão estática em `src/Page/dist`

A página é implementada em React + TypeScript e continua sendo 100% estática no artefato final. O `index.html` de publicação deve ser o gerado em `src/Page/dist`, não o arquivo fonte usado pelo Vite em `src/Page/index.html`.

## Publicação

- Os shards no R2 são publicados em releases versionados, por exemplo `files/shards/releases/{release_id}/{prefix}.ndjson`.
- Cada dataset publicado também gera um ZIP estável por dataset em `files/releases/{dataset}/data.zip` (ex.: `receita`, `cno`, `rntrc`), referenciado no `info.json` com tamanho e MD5.
- A API publicada consome os `*.ndjson` e `*.index.bin` do release ativo no R2, onde `prefix` usa os 3 primeiros caracteres do CNPJ normalizado.
- Cada linha do `*.ndjson` representa um CNPJ e o `*.index.bin` guarda `offset` e `length` exatos para leitura direta no Worker.
- O contrato de CNPJ já aceita formato alfanumérico: 12 caracteres alfanuméricos + 2 dígitos finais.
- O arquivo `info.json` continua sendo publicado com metadados do release ativo, `storage_release_id`, ZIP principal da Receita e o mapa `datasets` com metadados de ZIP por base.

## Deploy

- Use `src/script/deploy.sh` para orquestrar o release:
  - roda o ETL com release versionado
  - copia `info.json` e `*.index.bin` do release gerado para `src/Worker/assets`
  - executa `npm test` no Worker
  - faz `npx wrangler deploy`
  - valida `/info`, um CNPJ canônico e o mesmo CNPJ mascarado
  - remove o release antigo do bucket só depois da validação

## Contribuição

- Abra issues para discutir mudanças.
- Faça fork, crie uma branch descritiva e envie PR.
- Mantenha commits pequenos e o projeto compilando (`dotnet build`).
