import { getHotChunk, getHotIndex, getHotRuntimeInfo, rememberHotChunk, rememberHotIndex, rememberHotRuntimeInfo } from "./cache.ts";
import { parseBinaryShardIndex, findBinaryIndexEntry } from "./binary-index.ts";
import { createStageError } from "./errors.ts";
import { getEmbeddedRuntimeInfo, hasEmbeddedRuntimeInfo } from "./generated-runtime-info.ts";
import { jsonError, jsonOk, jsonOkNoStore } from "./http.ts";
import { R2_PUBLIC_ROOT, SHARD_PREFIX_LENGTH } from "./constants.ts";
import type { BinaryShardIndex, DatasetInfo, DatasetSelection, Env, RuntimeInfo } from "./types.ts";

export async function loadInfo(env: Env): Promise<Response> {
  const embedded = getEmbeddedRuntimeInfo();
  if (embedded) {
    return jsonOkNoStore(embedded);
  }

  const runtimeInfo = await loadRuntimeInfo(env);
  if (runtimeInfo) {
    return jsonOkNoStore(runtimeInfo);
  }

  return loadJsonFromR2(env.CNPJ_BUCKET, buildR2Key("info.json"));
}

export async function loadRecordFromShard(
  env: Env,
  bucket: R2Bucket,
  prefix: string,
  cnpj: string,
  runtimeInfo?: RuntimeInfo | null,
): Promise<Record<string, unknown> | null> {
  const resolvedRuntimeInfo = runtimeInfo ?? await loadRuntimeInfo(env);
  return loadDatasetsFromShard(
    env,
    bucket,
    prefix,
    cnpj,
    {
      includeReceita: true,
      moduleKeys: getModuleDatasetKeys(resolvedRuntimeInfo),
      cacheKey: "all",
    },
    resolvedRuntimeInfo);
}

export async function loadDatasetsFromShard(
  env: Env,
  bucket: R2Bucket,
  prefix: string,
  cnpj: string,
  selection: DatasetSelection,
  runtimeInfo?: RuntimeInfo | null,
): Promise<Record<string, unknown> | null> {
  const resolvedRuntimeInfo = runtimeInfo ?? await loadRuntimeInfo(env);
  const preferAssetIndexes = hasEmbeddedRuntimeInfo();
  let record: Record<string, unknown> = {};

  if (selection.includeReceita) {
    const receitaRecord = await loadReceitaRecordFromShard(env, bucket, prefix, cnpj, resolvedRuntimeInfo, preferAssetIndexes);
    if (receitaRecord == null) {
      return null;
    }

    record = receitaRecord;
  }

  await applyModuleShards(env, bucket, resolvedRuntimeInfo, prefix, cnpj, record, selection.moduleKeys, preferAssetIndexes);
  return record;
}

async function loadReceitaRecordFromShard(
  env: Env,
  bucket: R2Bucket,
  prefix: string,
  cnpj: string,
  runtimeInfo: RuntimeInfo | null,
  preferAssetIndex: boolean,
): Promise<Record<string, unknown> | null> {
  const releaseId = resolveShardReleaseId(runtimeInfo, prefix);
  const index = await loadBinaryIndex(env, prefix, releaseId, preferAssetIndex);
  if (!index || index.recordCount === 0) {
    return null;
  }

  const entry = findBinaryIndexEntry(index, cnpj);
  if (!entry) {
    return null;
  }

  const dataKey = buildR2Key(buildShardDataPath(prefix, releaseId));
  const chunk = await loadCachedRangeTextFromR2(bucket, dataKey, entry);
  if (chunk == null) {
    return null;
  }

  return parseExactNdjsonRecord(chunk, cnpj, dataKey);
}

export function getShardPrefix(cnpj: string): string {
  return cnpj.slice(0, SHARD_PREFIX_LENGTH);
}

export function buildRecordCacheKey(
  cnpj: string,
  prefix: string,
  runtimeInfo: RuntimeInfo | null,
  selection: DatasetSelection,
): string {
  const baseRelease = selection.includeReceita
    ? resolveShardReleaseId(runtimeInfo, prefix) ?? "assets"
    : "none";
  const moduleVersions = selection.moduleKeys
    .map(key => `${key}:${resolveModuleShardReleaseId(runtimeInfo?.datasets?.[key], prefix) ?? "none"}`)
    .join(",");

  return `https://cache.opencnpj/cnpj/${cnpj}?datasets=${encodeURIComponent(selection.cacheKey)}&v=${encodeURIComponent(`${baseRelease}|${moduleVersions}`)}`;
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

export async function loadRuntimeInfo(env: Env): Promise<RuntimeInfo | null> {
  const embedded = getEmbeddedRuntimeInfo();
  if (embedded) {
    return embedded;
  }

  const cached = getHotRuntimeInfo();
  if (cached) {
    return cached;
  }

  const text = await loadTextFromR2(env.CNPJ_BUCKET, buildR2Key("info.json"));
  if (text != null) {
    try {
      const parsed = JSON.parse(text) as RuntimeInfo;
      rememberHotRuntimeInfo(parsed);
      return parsed;
    } catch (error) {
      throw createStageError("info.r2.parse", error);
    }
  }

  const assetText = await loadTextFromAssets(env, buildAssetPath("info.json"));
  if (assetText == null) {
    return null;
  }

  try {
    const parsed = JSON.parse(assetText) as RuntimeInfo;
    rememberHotRuntimeInfo(parsed);
    return parsed;
  } catch (error) {
    throw createStageError("info.asset.parse", error);
  }
}

async function loadBinaryIndex(
  env: Env,
  prefix: string,
  releaseId?: string,
  preferAssetIndex = false,
): Promise<BinaryShardIndex | null> {
  const assetPath = buildAssetPath(`shards/${prefix}.index.bin`);
  if (preferAssetIndex) {
    const assetIndex = await loadBinaryIndexFromAssets(env, assetPath);
    if (assetIndex) {
      return assetIndex;
    }
  }

  if (releaseId) {
    const indexPath = buildR2Key(buildShardIndexPath(prefix, releaseId));
    const r2Index = await loadBinaryIndexFromR2(env.CNPJ_BUCKET, indexPath);
    if (r2Index) {
      return r2Index;
    }
  }

  return preferAssetIndex ? null : loadBinaryIndexFromAssets(env, assetPath);
}

async function loadModuleBinaryIndex(
  env: Env,
  bucket: R2Bucket,
  moduleKey: string,
  prefix: string,
  releaseId: string,
  preferAssetIndex: boolean,
): Promise<BinaryShardIndex | null> {
  if (preferAssetIndex) {
    const assetPath = buildAssetPath(buildModuleShardIndexPath(moduleKey, prefix, releaseId));
    const assetIndex = await loadBinaryIndexFromAssets(env, assetPath);
    if (assetIndex) {
      return assetIndex;
    }
  }

  const indexPath = buildR2Key(buildModuleShardIndexPath(moduleKey, prefix, releaseId));
  return loadBinaryIndexFromR2(bucket, indexPath);
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

async function loadBinaryIndexFromR2(bucket: R2Bucket, key: string): Promise<BinaryShardIndex | null> {
  const cached = getHotIndex(key);
  if (cached) {
    return cached;
  }

  let obj: R2ObjectBody | null;
  try {
    obj = await bucket.get(key);
  } catch (error) {
    throw createStageError(`r2.get:${key}`, error);
  }

  if (!obj) {
    return null;
  }

  let buffer: ArrayBuffer;
  try {
    buffer = await obj.arrayBuffer();
  } catch (error) {
    throw createStageError(`r2.arrayBuffer:${key}`, error);
  }

  const index = parseBinaryShardIndex(buffer, key);
  rememberHotIndex(key, index);
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

function resolveShardReleaseId(runtimeInfo: RuntimeInfo | null, _prefix: string): string | undefined {
  return runtimeInfo?.storage_release_id;
}

function resolveModuleShardReleaseId(moduleInfo: DatasetInfo | undefined, _prefix: string): string | undefined {
  return moduleInfo?.storage_release_id;
}

function getModuleDatasetKeys(runtimeInfo: RuntimeInfo | null): string[] {
  return Object.keys(runtimeInfo?.datasets ?? {})
    .filter(key => key !== "receita");
}

async function applyModuleShards(
  env: Env,
  bucket: R2Bucket,
  runtimeInfo: RuntimeInfo | null,
  prefix: string,
  cnpj: string,
  record: Record<string, unknown>,
  moduleKeys: string[],
  preferAssetIndexes: boolean,
): Promise<void> {
  const results = await Promise.all(moduleKeys.map(async moduleKey => {
    const moduleInfo = runtimeInfo?.datasets?.[moduleKey];
    const propertyName = moduleInfo?.json_property_name || moduleKey;
    const releaseId = resolveModuleShardReleaseId(moduleInfo, prefix);
    if (!releaseId) {
      return [propertyName, null] as const;
    }

    const payload = await loadModuleRecordFromShard(env, bucket, moduleKey, prefix, cnpj, releaseId, preferAssetIndexes);
    return [propertyName, payload] as const;
  }));

  for (const [propertyName, payload] of results) {
    record[propertyName] = payload;
  }
}

async function loadModuleRecordFromShard(
  env: Env,
  bucket: R2Bucket,
  moduleKey: string,
  prefix: string,
  cnpj: string,
  releaseId: string,
  preferAssetIndex: boolean,
): Promise<Record<string, unknown> | null> {
  const index = await loadModuleBinaryIndex(env, bucket, moduleKey, prefix, releaseId, preferAssetIndex);
  if (!index || index.recordCount === 0) {
    return null;
  }

  const entry = findBinaryIndexEntry(index, cnpj);
  if (!entry) {
    return null;
  }

  const dataKey = buildR2Key(buildModuleShardDataPath(moduleKey, prefix, releaseId));
  const chunk = await loadCachedRangeTextFromR2(bucket, dataKey, entry);
  if (chunk == null) {
    return null;
  }

  return parseExactNdjsonRecord(chunk, cnpj, dataKey);
}

function buildShardDataPath(prefix: string, releaseId?: string): string {
  return releaseId
    ? `shards/releases/${releaseId}/${prefix}.ndjson`
    : `shards/${prefix}.ndjson`;
}

function buildShardIndexPath(prefix: string, releaseId: string): string {
  return `shards/releases/${releaseId}/${prefix}.index.bin`;
}

function buildModuleShardDataPath(moduleKey: string, prefix: string, releaseId: string): string {
  return `shards/modules/${moduleKey}/${releaseId}/${prefix}.ndjson`;
}

function buildModuleShardIndexPath(moduleKey: string, prefix: string, releaseId: string): string {
  return `shards/modules/${moduleKey}/${releaseId}/${prefix}.index.bin`;
}

function parseExactNdjsonRecord(chunk: string, cnpj: string, key: string): Record<string, unknown> | null {
  const line = chunk.endsWith("\n") ? chunk.slice(0, -1) : chunk;
  if (!line) {
    return null;
  }

  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(line) as Record<string, unknown>;
  } catch (error) {
    throw createStageError(`shard-line.parse:${key}`, error);
  }

  return parsed.cnpj === cnpj ? parsed : null;
}
