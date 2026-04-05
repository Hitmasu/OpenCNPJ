# OpenCNPJ Worker

Worker Cloudflare que lê os índices binários publicados como Static Assets e os shards NDJSON publicados em releases versionados no R2.

## Estrutura

- `GET /info` lê `files/info.json` dos Static Assets do Worker, com fallback para R2.
- `GET /{cnpj}` normaliza o CNPJ para formato canônico, calcula o shard por prefixo de 3 caracteres, lê `files/shards/{prefix}.index.bin` dos Static Assets e usa o `shard_path_template` do `info.json` para buscar o `*.ndjson` do release ativo no R2.
- `OPTIONS` responde com CORS permissivo.

## Comportamento

- Aceita CNPJ numérico ou alfanumérico com máscara.
- Canonicaliza a chave de cache para evitar misses entre formatos como `12.345.678/0001-95` e `12345678000195`.
- Usa Cache API para a resposta final do endpoint.
- Usa cache quente em memória do isolate para índices de shard recentemente lidos.
- Assume que os artefatos publicados pelo ETL ficam sob o prefixo `files/`.
- O deploy coordenado por `src/script/deploy.sh` copia `info.json` e `src/ETL/Processor/cnpj_shards/{dataset}/releases/{release_id}/shards/*.index.bin` para `src/Worker/assets/files/` antes do `wrangler deploy`.

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
{"cnpj":"12345678000195","data":{"...":"..."}}
```

E um índice binário direto `prefix.index.bin`, ordenado por CNPJ, contendo `offset` e `length` de cada linha dentro do `prefix.ndjson`.
