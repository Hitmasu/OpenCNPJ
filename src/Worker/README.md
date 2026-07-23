# OpenCNPJ Worker

Worker Cloudflare que lê os índices binários publicados como Static Assets e os shards NDJSON publicados em releases versionados no R2.

## Estrutura

- `GET /info` lê `files/info.json` dos Static Assets do Worker, com fallback para R2.
- `GET /{cnpj}` retorna, por padrão, somente os dados da Receita. Para incluir módulos, use uma lista explícita, por exemplo `?datasets=receita,cno,rntrc`.
- A consulta normaliza o CNPJ, calcula o shard por prefixo de 3 caracteres, resolve o release por `storage_release_id`, tenta ler o índice binário no R2 em `files/shards/releases/{release}/{prefix}.index.bin` e, se não existir, cai para o asset legado `files/shards/{prefix}.index.bin`; depois busca o `*.ndjson` do mesmo release no R2.
- `OPTIONS` responde com CORS permissivo.

## Comportamento

- Aceita CNPJ numérico ou alfanumérico com máscara.
- Habilita o Workers Cache na frente do runtime, permitindo atender `GET`s cacheados sem invocar o Worker. O cache é isolado por versão do deploy.
- Mantém a Cache API como segundo nível para a resposta final do endpoint.
- Canonicaliza a chave desse segundo nível para evitar misses entre formatos como `12.345.678/0001-95` e `12345678000195`.
- Usa cache quente em memória do isolate para índices de shard recentemente lidos, tanto do R2 quanto dos Static Assets, limitado por quantidade e por 32 MiB.
- Mantém `/info` e `/schema` fora do cache com `Cache-Control: no-store`.
- Para maximizar hits no cache anterior ao runtime, clientes devem preferir o CNPJ sem máscara e `datasets` em ordem canônica: `receita,cno,rntrc`. A URL completa, inclusive a query string, compõe a chave desse cache.
- Assume que os artefatos publicados pelo ETL ficam sob o prefixo `files/`.
- O deploy coordenado por `src/scripts/deploy.sh` ainda copia `info.json` e os índices legados para `src/Worker/assets/files/` antes do `wrangler deploy`; quando o `info.json` publicar releases por prefixo, o Worker busca primeiro no R2.

## Deploy

1. Rode `src/scripts/deploy.sh` na raiz do repositório.
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
