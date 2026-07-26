import {
  getHotChunk,
  getHotIndex,
  getHotRoutingIndex,
  getHotRuntimeInfo,
  rememberHotChunk,
  rememberHotIndex,
  rememberHotRoutingIndex,
  rememberHotRuntimeInfo,
} from "./cache.ts";
import {
  parseBinaryShardIndex,
  findBinaryIndexEntries,
  findBinaryIndexEntry,
} from "./binary-index.ts";
import { createStageError } from "./errors.ts";
import { getEmbeddedRuntimeInfo, hasEmbeddedRuntimeInfo } from "./generated-runtime-info.ts";
import { jsonError, jsonOk, jsonOkNoStore } from "./http.ts";
import {
  findSegmentRoutingReferences,
  parseSegmentRoutingIndex,
} from "./routing-index.ts";
import { R2_PUBLIC_ROOT, SHARD_PREFIX_LENGTH } from "./constants.ts";
import type {
  BinaryIndexEntry,
  BinaryShardIndex,
  DatasetInfo,
  DatasetSelection,
  Env,
  RuntimeInfo,
  SegmentRoutingIndex,
} from "./types.ts";

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
    .map(key => `${key}:${resolveModuleCacheVersion(runtimeInfo?.datasets?.[key], prefix)}`)
    .join(",");

  return `https://cache.opencnpj/cnpj/${cnpj}?datasets=${encodeURIComponent(selection.cacheKey)}&v=${encodeURIComponent(`${baseRelease}|${moduleVersions}`)}`;
}

async function loadJsonFromR2(bucket: R2Bucket, key: string): Promise<Response> {
  const text = await loadTextFromR2(bucket, key);
  if (text == null) {
    return jsonError(404, "not found");
  }

  try {
    return jsonOkNoStore(JSON.parse(text));
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

function resolveModuleCacheVersion(
  moduleInfo: DatasetInfo | undefined,
  prefix: string,
): string {
  if (isSegmentedModule(moduleInfo)) {
    const segmentVersions = moduleInfo.segments
      .map(segment => `${segment.id}:${segment.storage_release_id}`)
      .join(".");
    return `${moduleInfo.routing_release_id}|${segmentVersions}`;
  }

  return resolveModuleShardReleaseId(moduleInfo, prefix) ?? "none";
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
    if (isSegmentedModule(moduleInfo)) {
      const payload = await loadSegmentedModuleRecord(
        bucket,
        moduleKey,
        prefix,
        cnpj,
        moduleInfo,
      );
      return [propertyName, payload] as const;
    }

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

async function loadSegmentedModuleRecord(
  bucket: R2Bucket,
  moduleKey: string,
  prefix: string,
  cnpj: string,
  moduleInfo: DatasetInfo & {
    routing_release_id: string;
    segments: NonNullable<DatasetInfo["segments"]>;
  },
): Promise<Record<string, unknown> | null> {
  const routing = await loadModuleRoutingIndex(
    bucket,
    moduleKey,
    prefix,
    moduleInfo.routing_release_id,
  );
  if (!routing) {
    return null;
  }

  const references = findSegmentRoutingReferences(routing, cnpj);
  if (references.length === 0) {
    return null;
  }

  const segmentsById = new Map(
    moduleInfo.segments.map(segment => [segment.id, segment]),
  );
  const orderedReferences = [...references]
    .sort((left, right) => left.segmentId.localeCompare(right.segmentId));
  const payloadGroups = await Promise.all(orderedReferences.map(async reference => {
    const segment = segmentsById.get(reference.segmentId);
    if (!segment) {
      throw new Error(
        `routing references unknown segment ${moduleKey}/${reference.segmentId}`,
      );
    }

    const dataPath = buildR2Key(buildSegmentModuleShardDataPath(
      moduleKey,
      reference.segmentId,
      segment.storage_release_id,
      prefix,
    ));
    const chunk = await loadCachedRangeTextFromR2(
      bucket,
      dataPath,
      {
        offset: reference.offset,
        length: reference.length,
      },
    );
    if (chunk == null) {
      throw new Error(`missing routed segment ${dataPath}`);
    }

    return parseExactNdjsonRecords(chunk, cnpj, dataPath);
  }));

  return mergeSegmentPayloads(
    cnpj,
    payloadGroups.flat(),
    moduleInfo.segment_collection_property,
  );
}

async function loadModuleRoutingIndex(
  bucket: R2Bucket,
  moduleKey: string,
  prefix: string,
  routingReleaseId: string,
): Promise<SegmentRoutingIndex | null> {
  const key = buildR2Key(
    buildModuleRoutingPath(moduleKey, prefix, routingReleaseId),
  );
  const cached = getHotRoutingIndex(key);
  if (cached) {
    return cached;
  }

  let object: R2ObjectBody | null;
  try {
    object = await bucket.get(key);
  } catch (error) {
    throw createStageError(`r2.get:${key}`, error);
  }

  if (!object) {
    return null;
  }

  const index = parseSegmentRoutingIndex(await object.arrayBuffer(), key);
  rememberHotRoutingIndex(key, index);
  return index;
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

  const entries = findBinaryIndexEntries(index, cnpj);
  if (entries.length === 0) {
    return null;
  }

  const dataKey = buildR2Key(buildModuleShardDataPath(moduleKey, prefix, releaseId));
  const payloadGroups = await Promise.all(
    coalesceAdjacentRanges(entries).map(async range => {
      const chunk = await loadCachedRangeTextFromR2(bucket, dataKey, range);
      if (chunk == null) {
        throw new Error(`missing routed module ${dataKey}`);
      }

      return parseExactNdjsonRecords(chunk, cnpj, dataKey);
    }),
  );

  return mergeSegmentPayloads(cnpj, payloadGroups.flat());
}

function coalesceAdjacentRanges(
  entries: BinaryIndexEntry[],
): BinaryIndexEntry[] {
  const ordered = [...entries].sort(
    (left, right) => left.offset - right.offset,
  );
  const ranges: BinaryIndexEntry[] = [];
  for (const entry of ordered) {
    const current = ranges.at(-1);
    if (current && current.offset + current.length === entry.offset) {
      current.length += entry.length;
    } else {
      ranges.push({ ...entry });
    }
  }

  return ranges;
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

function buildModuleRoutingPath(
  moduleKey: string,
  prefix: string,
  routingReleaseId: string,
): string {
  return `shards/modules/${moduleKey}/routing/${routingReleaseId}/${prefix}.routing.bin`;
}

function buildSegmentModuleShardDataPath(
  moduleKey: string,
  segmentId: string,
  releaseId: string,
  prefix: string,
): string {
  return `shards/modules/${moduleKey}/segments/${segmentId}/${releaseId}/${prefix}.ndjson`;
}

function isSegmentedModule(
  moduleInfo: DatasetInfo | undefined,
): moduleInfo is DatasetInfo & {
  routing_release_id: string;
  segments: NonNullable<DatasetInfo["segments"]>;
} {
  return Boolean(
    moduleInfo?.routing_release_id
    && moduleInfo.segments
    && moduleInfo.segments.length > 0,
  );
}

function mergeSegmentPayloads(
  cnpj: string,
  payloads: Record<string, unknown>[],
  collectionProperty?: string,
): Record<string, unknown> | null {
  if (payloads.length === 0) {
    return null;
  }

  const merged: Record<string, unknown> = { cnpj };
  for (const payload of payloads) {
    mergeObject(merged, payload);
  }

  if (
    collectionProperty
    && !Array.isArray(merged[collectionProperty])
  ) {
    merged[collectionProperty] = [];
  }

  merged.cnpj = cnpj;
  return merged;
}

function mergeObject(
  target: Record<string, unknown>,
  source: Record<string, unknown>,
): void {
  for (const [key, value] of Object.entries(source)) {
    if (key === "cnpj") {
      continue;
    }

    const current = target[key];
    if (Array.isArray(value)) {
      target[key] = appendUnique(
        Array.isArray(current) ? current : [],
        value,
      );
    } else if (isPlainObject(value)) {
      const nested = isPlainObject(current) ? current : {};
      mergeObject(nested, value);
      target[key] = nested;
    } else if (
      key !== "updated_at"
      || typeof current !== "string"
      || typeof value !== "string"
      || value.localeCompare(current) >= 0
    ) {
      target[key] = value;
    }
  }
}

function appendUnique(current: unknown[], additions: unknown[]): unknown[] {
  const result = [...current];
  const seen = new Set(current.map(value => JSON.stringify(value)));
  for (const value of additions) {
    const identity = JSON.stringify(value);
    if (seen.has(identity)) {
      continue;
    }

    seen.add(identity);
    result.push(value);
  }

  return result;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value != null && typeof value === "object" && !Array.isArray(value);
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

function parseExactNdjsonRecords(
  chunk: string,
  cnpj: string,
  key: string,
): Record<string, unknown>[] {
  const lines = chunk.split("\n").filter(line => line.length > 0);
  return lines.map((line, index) => {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(line) as Record<string, unknown>;
    } catch (error) {
      throw createStageError(`shard-lines.parse:${key}:${index}`, error);
    }

    if (parsed.cnpj !== cnpj) {
      throw createStageError(
        `shard-lines.cnpj:${key}:${index}`,
        new Error(`expected ${cnpj}`),
      );
    }

    return parsed;
  });
}
