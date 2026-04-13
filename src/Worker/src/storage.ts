import { getHotChunk, getHotIndex, getHotRuntimeInfo, rememberHotChunk, rememberHotIndex, rememberHotRuntimeInfo } from "./cache.ts";
import { parseBinaryShardIndex, findBinaryIndexEntry } from "./binary-index.ts";
import { createStageError } from "./errors.ts";
import { jsonError, jsonOk } from "./http.ts";
import { R2_PUBLIC_ROOT, SHARD_PREFIX_LENGTH } from "./constants.ts";
import type { BinaryShardIndex, Env, NdjsonShardLine, RuntimeInfo } from "./types.ts";

export async function loadInfo(env: Env): Promise<Response> {
  const runtimeInfo = await loadRuntimeInfo(env);
  if (runtimeInfo) {
    return jsonOk(runtimeInfo);
  }

  return loadJsonFromR2(env.CNPJ_BUCKET, buildR2Key("info.json"));
}

export async function loadRecordFromShard(
  env: Env,
  bucket: R2Bucket,
  prefix: string,
  cnpj: string,
): Promise<Record<string, unknown> | null> {
  const index = await loadBinaryIndexFromAssets(env, buildAssetPath(`shards/${prefix}.index.bin`));
  if (!index || index.recordCount === 0) {
    return null;
  }

  const runtimeInfo = await loadRuntimeInfo(env);
  const shardTemplate = runtimeInfo?.shard_path_template ?? "shards/{prefix}.ndjson";

  const entry = findBinaryIndexEntry(index, cnpj);
  if (!entry) {
    return null;
  }

  const dataKey = buildR2Key(resolveShardDataPath(shardTemplate, prefix, runtimeInfo?.storage_release_id));
  const chunk = await loadCachedRangeTextFromR2(bucket, dataKey, entry);
  if (chunk == null) {
    return null;
  }

  return parseExactNdjsonRecord(chunk, cnpj, dataKey);
}

export function getShardPrefix(cnpj: string): string {
  return cnpj.slice(0, SHARD_PREFIX_LENGTH);
}

async function loadJsonFromR2(bucket: R2Bucket, key: string): Promise<Response> {
  const text = await loadTextFromR2(bucket, key);
  if (text == null) {
    return jsonError(404, "not found");
  }

  try {
    return jsonOk(JSON.parse(text));
  } catch (error) {
    throw createStageError("info.json.parse", error);
  }
}

async function loadRuntimeInfo(env: Env): Promise<RuntimeInfo | null> {
  const cached = getHotRuntimeInfo();
  if (cached) {
    return cached;
  }

  const assetText = await loadTextFromAssets(env, buildAssetPath("info.json"));
  if (assetText != null) {
    try {
      const parsed = JSON.parse(assetText) as RuntimeInfo;
      rememberHotRuntimeInfo(parsed);
      return parsed;
    } catch (error) {
      throw createStageError("info.asset.parse", error);
    }
  }

  const text = await loadTextFromR2(env.CNPJ_BUCKET, buildR2Key("info.json"));
  if (text == null) {
    return null;
  }

  try {
    const parsed = JSON.parse(text) as RuntimeInfo;
    rememberHotRuntimeInfo(parsed);
    return parsed;
  } catch (error) {
    throw createStageError("info.r2.parse", error);
  }
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

async function loadBinaryIndexFromAssets(env: Env, assetPath: string): Promise<BinaryShardIndex | null> {
  const cached = getHotIndex(assetPath);
  if (cached) {
    return cached;
  }

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

  let buffer: ArrayBuffer;
  try {
    buffer = await response.arrayBuffer();
  } catch (error) {
    throw createStageError(`assets.arrayBuffer:${assetPath}`, error);
  }

  const index = parseBinaryShardIndex(buffer, assetPath);
  rememberHotIndex(assetPath, index);
  return index;
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

async function loadCachedRangeTextFromR2(
  bucket: R2Bucket,
  key: string,
  range: { offset: number; length: number },
): Promise<string | null> {
  const cacheKey = `${key}:${range.offset}+${range.length}`;
  const cached = getHotChunk(cacheKey);
  if (cached != null) {
    return cached;
  }

  const text = await loadTextFromR2(bucket, key, range);
  if (text != null) {
    rememberHotChunk(cacheKey, text);
  }

  return text;
}

function buildR2Key(relativeKey: string): string {
  const normalized = relativeKey.replace(/^\/+/, "");
  return normalized.startsWith(`${R2_PUBLIC_ROOT}/`)
    ? normalized
    : `${R2_PUBLIC_ROOT}/${normalized}`;
}

function buildAssetPath(relativeKey: string): string {
  return `/${R2_PUBLIC_ROOT}/${relativeKey}`.replace(/\/{2,}/g, "/");
}

function resolveShardDataPath(template: string, prefix: string, releaseId?: string): string {
  return template
    .replaceAll("{prefix}", prefix)
    .replaceAll("{release_id}", releaseId ?? "");
}

function parseExactNdjsonRecord(chunk: string, cnpj: string, key: string): Record<string, unknown> | null {
  const line = chunk.endsWith("\n") ? chunk.slice(0, -1) : chunk;
  if (!line) {
    return null;
  }

  let parsed: NdjsonShardLine;
  try {
    parsed = JSON.parse(line) as NdjsonShardLine;
  } catch (error) {
    throw createStageError(`shard-line.parse:${key}`, error);
  }

  return parsed.cnpj === cnpj ? parsed.data : null;
}

