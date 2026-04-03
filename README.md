<img src="./Page/assets/logo.svg" alt="OpenCNPJ" height="64" />

Projeto aberto para baixar, processar e publicar dados públicos das empresas do Brasil.

## Pastas

- `ETL`: ETL que baixa, processa e publica dados do CNPJ.
- `Page`: página/SPA estática para consulta dos dados publicados.
- `Worker`: placeholder do Cloudflare Worker que lê shards publicados no R2.

## Requisitos

- `.NET SDK 10.0+`
- `rclone` instalado e autenticado no seu storage (ex.: Backblaze, R2, S3, Azure Storage, ...).
- Espaço em disco e boa conexão (a primeira execução pode levar tempo -- dias até).

## Configuração

- Ajuste `ETL/config.json` se desejar mudar pastas locais, destino do storage, memória, paralelismo... 
- No `config.json`, aponte para o Storage que deseja passando a configuração do rclone.
- O downloader da Receita agora usa WebDAV no share público do SERPRO+/Nextcloud.

## Layout local

- `downloads/YYYY-MM`: zips baixados da Receita.
- `extracted_data/YYYY-MM`: arquivos extraídos para o mês.
- `parquet_data/YYYY-MM`: Parquets gerados para o mês.
- `cnpj_shards/YYYY-MM/shards`: shards locais `*.ndjson` e `*.index.json` antes do upload.

Os artefatos locais não são apagados automaticamente. O pipeline também não usa mais cache de hash por shard.

## Execução

- Dentro de `ETL`:
  - `dotnet run pipeline`
  - `dotnet run pipeline -m YYYY-MM` (opcional)

Sem `-m`, o pipeline escolhe o mês mais recente publicado no share WebDAV da Receita.

## Publicação

- A API publicada consome shards em `shards/{prefix}.ndjson` no R2 e `shards/{prefix}.index.json` como Static Asset do Worker, onde `prefix` usa os 3 primeiros caracteres do CNPJ normalizado.
- Cada linha do `*.ndjson` representa um CNPJ e o `*.index.json` guarda offsets esparsos para leitura parcial no Worker.
- O contrato de CNPJ já aceita formato alfanumérico: 12 caracteres alfanuméricos + 2 dígitos finais.
- O arquivo `info.json` continua sendo publicado, agora com metadados adicionais de shard.

## Contribuição

- Abra issues para discutir mudanças.
- Faça fork, crie uma branch descritiva e envie PR.
- Mantenha commits pequenos e o projeto compilando (`dotnet build`).
