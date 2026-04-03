export interface Env {
  CNPJ_BUCKET: R2Bucket;
  ASSETS?: Fetcher;
}

type SparseIndexEntry = {
  cnpj: string;
  offset: number;
};

type SparseIndexDocument = {
  version: number;
  format: string;
  prefix: string;
  data_file: string;
  stride: number;
  record_count: number;
  data_size: number;
  entries: SparseIndexEntry[];
};

type HotIndexCacheEntry = {
  expiresAt: number;
  index: SparseIndexDocument;
};

type NdjsonShardLine = {
  cnpj: string;
  data: Record<string, unknown>;
};

const JSON_HEADERS = {
  "Content-Type": "application/json; charset=utf-8",
  "Access-Control-Allow-Origin": "*",
};

const NO_STORE_HEADERS = {
  ...JSON_HEADERS,
  "Cache-Control": "no-store",
};

const CACHE_TTL_SECONDS = 60 * 60 * 24;
const HOT_INDEX_CACHE_TTL_MS = 30 * 60 * 1000;
const HOT_INDEX_CACHE_MAX_ENTRIES = 256;
const SHARD_PREFIX_LENGTH = 3;
const CNPJ_MASK_CHARACTERS = /[./-]/g;
const ALPHANUMERIC_CNPJ_PATTERN = /^[A-Z0-9]{12}\d{2}$/;
const R2_PUBLIC_ROOT = "files";
const hotIndexCache = new Map<string, HotIndexCacheEntry>();

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
      const canonicalInfoUrl = buildCanonicalInfoUrl(url);
      return handleCachedJson(canonicalInfoUrl, ctx, async () => {
        try {
          return await loadInfo(env);
        } catch (error) {
          console.error("info load failed", error);
          return jsonError(502, "info load failed");
        }
      });
    }

    const cnpj = extractCnpjFromPath(pathname);
    if (!cnpj) {
      return jsonError(400, "invalid cnpj");
    }

    const canonicalCnpjUrl = buildCanonicalCnpjUrl(url, cnpj);
    return handleCachedJson(canonicalCnpjUrl, ctx, async () => {
      try {
        const prefix = cnpj.slice(0, SHARD_PREFIX_LENGTH);
        const record = await loadRecordFromShard(env, env.CNPJ_BUCKET, prefix, cnpj);
        if (record == null) {
          return jsonError(404, "not found");
        }

        return jsonOk(record);
      } catch (error) {
        console.error("cnpj lookup failed", {
          cnpj,
          prefix: cnpj.slice(0, SHARD_PREFIX_LENGTH),
          error,
        });
        return jsonError(502, "invalid shard payload");
      }
    });
  },
};

async function handleCachedJson(
  cacheUrl: URL,
  ctx: ExecutionContext,
  loader: () => Promise<Response>,
): Promise<Response> {
  const cache = caches.default;
  const cacheKey = new Request(cacheUrl.toString(), { method: "GET" });
  const cached = await cache.match(cacheKey);
  if (cached) {
    return cached;
  }

  const response = await loader();
  if (response.ok) {
    ctx.waitUntil(cache.put(cacheKey, response.clone()));
  }

  return response;
}

async function loadJsonFromR2(bucket: R2Bucket, key: string): Promise<Response> {
  const text = await loadTextFromR2(bucket, key);
  if (text == null) {
    return jsonError(404, "not found");
  }

  try {
    const data = JSON.parse(text);
    return jsonOk(data);
  } catch (error) {
    throw createStageError("info.json.parse", error);
  }
}

async function loadInfo(env: Env): Promise<Response> {
  const assetText = await loadTextFromAssets(env, buildAssetPath("info.json"));
  if (assetText != null) {
    try {
      return jsonOk(JSON.parse(assetText));
    } catch (error) {
      throw createStageError("info.asset.parse", error);
    }
  }

  return loadJsonFromR2(env.CNPJ_BUCKET, buildR2Key("info.json"));
}

async function loadTextFromAssets(env: Env, assetPath: string): Promise<string | null> {
  if (!env.ASSETS) {
    return null;
  }

  let response: Response;
  try {
    response = await env.ASSETS.fetch(new Request(`https://assets.invalid${assetPath}`));
  } catch (error) {
    throw createStageError(`assets.fetch:${assetPath}`, error);
  }

  if (response.status === 404) {
    return null;
  }

  if (!response.ok) {
    throw createStageError(`assets.fetch:${assetPath}`, new Error(`Unexpected status code ${response.status}`));
  }

  try {
    return await response.text();
  } catch (error) {
    throw createStageError(`assets.text:${assetPath}`, error);
  }
}

async function loadTextFromR2(
  bucket: R2Bucket,
  key: string,
  range?: { offset: number; length: number },
): Promise<string | null> {
  let obj: R2ObjectBody | null;
  try {
    obj = range ? await bucket.get(key, { range }) : await bucket.get(key);
  } catch (error) {
    const stage = range
      ? `r2.get:${key}:${range.offset}+${range.length}`
      : `r2.get:${key}`;
    throw createStageError(stage, error);
  }

  if (!obj) {
    return null;
  }

  try {
    return await obj.text();
  } catch (error) {
    const stage = range
      ? `r2.text:${key}:${range.offset}+${range.length}`
      : `r2.text:${key}`;
    throw createStageError(stage, error);
  }
}

async function loadShardIndexFromR2(bucket: R2Bucket, key: string): Promise<SparseIndexDocument | null> {
  const cached = getHotIndex(key);
  if (cached) {
    return cached;
  }

  const text = await loadTextFromR2(bucket, key);
  if (text == null) {
    return null;
  }

  let index: SparseIndexDocument;
  try {
    index = JSON.parse(text) as SparseIndexDocument;
  } catch (error) {
    throw createStageError(`shard-index.parse:${key}`, error);
  }

  rememberHotIndex(key, index);
  return index;
}

async function loadShardIndex(
  env: Env,
  bucket: R2Bucket,
  key: string,
  assetPath: string,
): Promise<SparseIndexDocument | null> {
  const cached = getHotIndex(assetPath);
  if (cached) {
    return cached;
  }

  const assetText = await loadTextFromAssets(env, assetPath);
  if (assetText != null) {
    let assetIndex: SparseIndexDocument;
    try {
      assetIndex = JSON.parse(assetText) as SparseIndexDocument;
    } catch (error) {
      throw createStageError(`shard-index.asset.parse:${assetPath}`, error);
    }

    rememberHotIndex(assetPath, assetIndex);
    return assetIndex;
  }

  return loadShardIndexFromR2(bucket, key);
}

async function loadRecordFromShard(
  env: Env,
  bucket: R2Bucket,
  prefix: string,
  cnpj: string,
): Promise<Record<string, unknown> | null> {
  const indexKey = buildR2Key(`shards/${prefix}.index.json`);
  const assetPath = buildAssetPath(`shards/${prefix}.index.json`);
  const index = await loadShardIndex(env, bucket, indexKey, assetPath);
  if (!index || index.entries.length === 0) {
    return null;
  }

  const anchorIndex = findAnchorIndex(index.entries, cnpj);
  if (anchorIndex < 0) {
    return null;
  }

  const startOffset = index.entries[anchorIndex].offset;
  const endOffset = anchorIndex + 1 < index.entries.length
    ? index.entries[anchorIndex + 1].offset
    : index.data_size;

  const length = Math.max(0, endOffset - startOffset);
  if (length <= 0) {
    return null;
  }

  const dataKey = buildR2Key(`shards/${index.data_file}`);
  const chunk = await loadTextFromR2(bucket, dataKey, { offset: startOffset, length });
  if (chunk == null) {
    return null;
  }

  return findRecordInNdjsonChunk(chunk, cnpj, dataKey);
}

function buildR2Key(relativeKey: string): string {
  return `${R2_PUBLIC_ROOT}/${relativeKey}`.replace(/^\/+/, "");
}

function buildAssetPath(relativeKey: string): string {
  return `/${R2_PUBLIC_ROOT}/${relativeKey}`.replace(/\/{2,}/g, "/");
}

function extractCnpjFromPath(pathname: string): string | null {
  const segments = pathname.split("/").filter(Boolean);
  if (segments.length !== 1) {
    return null;
  }

  return normalizeCnpj(segments[0]);
}

function normalizeCnpj(value: string): string | null {
  const decoded = safeDecodeURIComponent(value);
  const normalized = decoded.trim().toUpperCase().replace(CNPJ_MASK_CHARACTERS, "");
  return ALPHANUMERIC_CNPJ_PATTERN.test(normalized) ? normalized : null;
}

function safeDecodeURIComponent(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function buildCanonicalInfoUrl(url: URL): URL {
  const canonical = new URL(url.toString());
  canonical.pathname = "/info";
  canonical.search = "";
  canonical.hash = "";
  return canonical;
}

function buildCanonicalCnpjUrl(url: URL, cnpj: string): URL {
  const canonical = new URL(url.toString());
  canonical.pathname = `/${cnpj}`;
  canonical.search = "";
  canonical.hash = "";
  return canonical;
}

function findAnchorIndex(entries: SparseIndexEntry[], cnpj: string): number {
  let low = 0;
  let high = entries.length - 1;
  let result = -1;

  while (low <= high) {
    const mid = (low + high) >> 1;
    const current = entries[mid].cnpj;

    if (current.localeCompare(cnpj) <= 0) {
      result = mid;
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return result;
}

function findRecordInNdjsonChunk(chunk: string, cnpj: string, key: string): Record<string, unknown> | null {
  const lines = chunk.split("\n");

  for (const line of lines) {
    if (!line) {
      continue;
    }

    let parsed: NdjsonShardLine;
    try {
      parsed = JSON.parse(line) as NdjsonShardLine;
    } catch (error) {
      throw createStageError(`shard-line.parse:${key}`, error);
    }

    if (parsed.cnpj === cnpj) {
      return parsed.data;
    }

    if (parsed.cnpj.localeCompare(cnpj) > 0) {
      return null;
    }
  }

  return null;
}

function getHotIndex(key: string): SparseIndexDocument | null {
  const entry = hotIndexCache.get(key);
  if (!entry) {
    return null;
  }

  if (entry.expiresAt <= Date.now()) {
    hotIndexCache.delete(key);
    return null;
  }

  hotIndexCache.delete(key);
  hotIndexCache.set(key, entry);
  return entry.index;
}

function rememberHotIndex(key: string, index: SparseIndexDocument): void {
  hotIndexCache.delete(key);
  hotIndexCache.set(key, {
    index,
    expiresAt: Date.now() + HOT_INDEX_CACHE_TTL_MS,
  });

  while (hotIndexCache.size > HOT_INDEX_CACHE_MAX_ENTRIES) {
    const oldestKey = hotIndexCache.keys().next().value;
    if (oldestKey == null) {
      break;
    }

    hotIndexCache.delete(oldestKey);
  }
}

function jsonOk(data: unknown): Response {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: {
      ...JSON_HEADERS,
      "Cache-Control": `public, max-age=${CACHE_TTL_SECONDS}, s-maxage=${CACHE_TTL_SECONDS}`,
    },
  });
}

function jsonError(status: number, message: string): Response {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: NO_STORE_HEADERS,
  });
}

function createStageError(stage: string, error: unknown): Error {
  if (error instanceof Error) {
    return new Error(`[${stage}] ${error.message}`, { cause: error });
  }

  return new Error(`[${stage}] ${typeof error === "string" ? error : JSON.stringify(error)}`);
}

function corsPreflight(): Response {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Accept",
      "Access-Control-Max-Age": String(CACHE_TTL_SECONDS),
    },
  });
}
