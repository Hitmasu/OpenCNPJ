import { SHARD_PREFIX_LENGTH } from "./constants.ts";
import { extractCnpjFromPath, normalizeCnpj } from "./cnpj.ts";
import { clearHotCaches } from "./cache.ts";
import { resolveDatasetSelection } from "./datasets.ts";
import { setEmbeddedRuntimeInfoForTest } from "./generated-runtime-info.ts";
import { handleCachedJson, corsPreflight, jsonError, jsonOk } from "./http.ts";
import { buildRecordCacheKey, getShardPrefix, loadDatasetsFromShard, loadInfo, loadRuntimeInfo } from "./storage.ts";
import type { RuntimeInfo } from "./types.ts";
import type { Env } from "./types.ts";

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
