import { BATCH_MAX_CNPJS, SHARD_PREFIX_LENGTH } from "./constants.ts";
import { extractCnpjFromPath, normalizeCnpj } from "./cnpj.ts";
import { clearHotCaches } from "./cache.ts";
import { resolveDatasetSelection } from "./datasets.ts";
import { setEmbeddedRuntimeInfoForTest } from "./generated-runtime-info.ts";
import { handleCachedJson, corsPreflight, jsonError, jsonOk } from "./http.ts";
import { loadSchema } from "./schema.ts";
import { buildRecordCacheKey, getShardPrefix, loadDatasetsFromShard, loadInfo, loadRuntimeInfo } from "./storage.ts";
import type { RuntimeInfo } from "./types.ts";
import type { Env } from "./types.ts";


/**
 * Consulta em lote: `GET /batch?cnpjs=a,b,c`
 *
 * Quem processa documentos consulta dezenas ou centenas de CNPJs de uma vez, e
 * hoje isso custa uma requisicao por CNPJ. O ganho nao e so de rede: os
 * registros ficam em shards por prefixo, entao agrupar o lote por prefixo faz
 * cada shard ser aberto UMA vez em vez de uma vez por CNPJ.
 *
 * Continua sendo GET, como o resto da API, para permanecer cacheavel e manter
 * a mesma superficie (o Worker so aceita GET e OPTIONS).
 */
async function handleBatch(
  url: URL,
  env: Env,
): Promise<Response> {
  const bruto = url.searchParams.get("cnpjs");
  if (!bruto) {
    return jsonError(400, "missing cnpjs");
  }

  const pedidos = bruto.split(",").map((parte) => parte.trim()).filter(Boolean);
  if (pedidos.length === 0) {
    return jsonError(400, "missing cnpjs");
  }
  if (pedidos.length > BATCH_MAX_CNPJS) {
    return jsonError(400, `too many cnpjs (max ${BATCH_MAX_CNPJS})`);
  }

  // Normaliza e remove repetidos preservando a ordem pedida. Repetir o mesmo
  // CNPJ no lote e comum quando a lista vem de nomes de arquivo.
  const validos: string[] = [];
  const invalidos: string[] = [];
  const vistos = new Set<string>();
  for (const pedido of pedidos) {
    const cnpj = normalizeCnpj(pedido);
    if (!cnpj) {
      invalidos.push(pedido);
      continue;
    }
    if (!vistos.has(cnpj)) {
      vistos.add(cnpj);
      validos.push(cnpj);
    }
  }

  const runtimeInfo = await loadRuntimeInfo(env);
  const selection = resolveDatasetSelection(url.searchParams, runtimeInfo);
  if (!selection.ok) {
    return jsonError(400, selection.error);
  }

  // Agrupado por shard: e daqui que vem a economia de leitura no R2.
  const porPrefixo = new Map<string, string[]>();
  for (const cnpj of validos) {
    const prefixo = getShardPrefix(cnpj);
    const lista = porPrefixo.get(prefixo);
    if (lista) {
      lista.push(cnpj);
    } else {
      porPrefixo.set(prefixo, [cnpj]);
    }
  }

  const encontrados: unknown[] = [];
  const naoEncontrados: string[] = [];
  for (const [prefixo, doPrefixo] of porPrefixo) {
    for (const cnpj of doPrefixo) {
      const record = await loadDatasetsFromShard(
        env,
        env.CNPJ_BUCKET,
        prefixo,
        cnpj,
        selection.value,
        runtimeInfo,
      );
      if (record == null) {
        naoEncontrados.push(cnpj);
      } else {
        encontrados.push(record);
      }
    }
  }

  return jsonOk({
    solicitados: pedidos.length,
    encontrados,
    nao_encontrados: naoEncontrados,
    invalidos,
  });
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (request.method === "OPTIONS") {
      return corsPreflight();
    }

    if (request.method !== "GET") {
      return jsonError(405, "method not allowed");
    }

    const url = new URL(request.url);
    const pathname = url.pathname.replace(/\/+$/, "");

    if (pathname === "/info") {
      try {
        return await loadInfo(env);
      } catch (error) {
        console.error("info load failed", error);
        return jsonError(502, "info load failed");
      }
    }

    if (pathname === "/batch") {
      try {
        return await handleBatch(url, env);
      } catch (error) {
        console.error("batch lookup failed", error);
        return jsonError(502, "batch lookup failed");
      }
    }

    if (pathname === "/schema") {
      return loadSchema();
    }

    const cnpj = extractCnpjFromPath(pathname);
    if (!cnpj) {
      return jsonError(400, "invalid cnpj");
    }

    try {
      const prefix = getShardPrefix(cnpj);
      const runtimeInfo = await loadRuntimeInfo(env);
      const selection = resolveDatasetSelection(url.searchParams, runtimeInfo);
      if (!selection.ok) {
        return jsonError(400, selection.error);
      }

      const cacheKey = buildRecordCacheKey(cnpj, prefix, runtimeInfo, selection.value);

      return await handleCachedJson(cacheKey, ctx, async () => {
        const record = await loadDatasetsFromShard(
          env,
          env.CNPJ_BUCKET,
          prefix,
          cnpj,
          selection.value,
          runtimeInfo);
        if (record == null) {
          return jsonError(404, "not found");
        }

        return jsonOk(record);
      });
    } catch (error) {
      console.error("cnpj lookup failed", {
        cnpj,
        prefix: cnpj.slice(0, SHARD_PREFIX_LENGTH),
        error,
      });
      return jsonError(502, "invalid shard payload");
    }
  },
};

export const __test__ = {
  extractCnpjFromPath,
  normalizeCnpj,
  clearHotIndexCache(): void {
    clearHotCaches();
  },
  setEmbeddedRuntimeInfoForTest(value: RuntimeInfo | null | undefined): void {
    setEmbeddedRuntimeInfoForTest(value);
  },
};
