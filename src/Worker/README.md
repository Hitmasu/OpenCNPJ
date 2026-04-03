# OpenCNPJ Worker

Worker Cloudflare que lê os índices publicados como Static Assets e os shards NDJSON publicados em R2.

## Estrutura

- `GET /info` lê `files/info.json` dos Static Assets do Worker, com fallback para R2.
- `GET /{cnpj}` normaliza o CNPJ para formato canônico, calcula o shard por prefixo de 3 caracteres, lê `files/shards/{prefix}.index.json` dos Static Assets e depois busca apenas o range necessário em `files/shards/{prefix}.ndjson` no R2.
- `OPTIONS` responde com CORS permissivo.

## Comportamento

- Aceita CNPJ numérico ou alfanumérico com máscara.
- Canonicaliza a chave de cache para evitar misses entre formatos como `12.345.678/0001-95` e `12345678000195`.
- Usa Cache API para a resposta final do endpoint.
- Usa cache quente em memória do isolate para índices de shard recentemente lidos.
- Assume que os artefatos publicados pelo ETL ficam sob o prefixo `files/`.
- O pipeline do ETL prepara automaticamente `src/Worker/assets/files/info.json` e `src/Worker/assets/files/shards/*.index.json` para o próximo deploy.

## Deploy

1. Rode a pipeline do ETL em `src/ETL/Processor` para preparar `src/Worker/assets/` com os índices e o `info.json` do dataset atual.
2. Ajuste `bucket_name` em [`wrangler.toml`](./wrangler.toml) para o nome real do bucket R2.
3. Execute `wrangler deploy` dentro da pasta `src/Worker/`.

## Observação

O contrato do runtime assume dois artefatos por shard:

```json
{"cnpj":"12345678000195","data":{"...":"..."}}
```

E um índice esparso `prefix.index.json` apontando offsets dentro do `prefix.ndjson`.
