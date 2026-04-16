# OpenCNPJ Worker

Worker Cloudflare que lê os índices binários publicados como Static Assets e os shards NDJSON publicados em releases versionados no R2.

## Estrutura

- `GET /info` lê `files/info.json` dos Static Assets do Worker, com fallback para R2.
- `GET /{cnpj}` normaliza o CNPJ para formato canônico, calcula o shard por prefixo de 3 caracteres, resolve o release pelo `shard_releases[prefix]`, `default_shard_release_id` ou `storage_release_id`, tenta ler o índice binário no R2 em `files/shards/releases/{release}/{prefix}.index.bin` e, se não existir, cai para o asset legado `files/shards/{prefix}.index.bin`; depois busca o `*.ndjson` do mesmo release no R2.
- `OPTIONS` responde com CORS permissivo.

## Comportamento

- Aceita CNPJ numérico ou alfanumérico com máscara.
- Canonicaliza a chave de cache para evitar misses entre formatos como `12.345.678/0001-95` e `12345678000195`.
- Usa Cache API para a resposta final do endpoint.
- Usa cache quente em memória do isolate para índices de shard recentemente lidos, tanto do R2 quanto dos Static Assets.
- Assume que os artefatos publicados pelo ETL ficam sob o prefixo `files/`.
- O deploy coordenado por `src/script/deploy.sh` ainda copia `info.json` e os índices legados para `src/Worker/assets/files/` antes do `wrangler deploy`; quando o `info.json` publicar releases por prefixo, o Worker busca primeiro no R2.

## Deploy

1. Rode `src/script/deploy.sh` na raiz do repositório.
2. Ajuste `bucket_name` em [`wrangler.toml`](./wrangler.toml) para o nome real do bucket R2.
3. Opcionalmente, informe `--base-url` ao script se quiser validar e limpar o release antigo usando um domínio específico.

## Testes

Dentro de `src/Worker`, rode:

```bash
npm test
```

## Observação

O contrato do runtime assume dois artefatos por shard:

```json
{"cnpj":"12345678000195","...":"..."}
```

E um índice binário direto `prefix.index.bin`, ordenado por CNPJ, contendo `offset` e `length` de cada linha dentro do `prefix.ndjson`.
