import {
  HOT_CHUNK_CACHE_MAX_BYTES,
  HOT_CHUNK_CACHE_MAX_ENTRIES,
  HOT_CHUNK_CACHE_TTL_MS,
  HOT_INDEX_CACHE_MAX_ENTRIES,
  HOT_INDEX_CACHE_TTL_MS,
  HOT_RUNTIME_INFO_TTL_MS,
} from "./constants.ts";
import type { BinaryShardIndex, RuntimeInfo } from "./types.ts";

type HotIndexCacheEntry = {
  expiresAt: number;
  index: BinaryShardIndex;
};

type HotChunkCacheEntry = {
  expiresAt: number;
  value: string;
  size: number;
};

const hotIndexCache = new Map<string, HotIndexCacheEntry>();
const hotChunkCache = new Map<string, HotChunkCacheEntry>();
let hotChunkCacheBytes = 0;
let hotRuntimeInfo: { expiresAt: number; value: RuntimeInfo } | null = null;

export function clearHotCaches(): void {
  hotIndexCache.clear();
  hotChunkCache.clear();
  hotChunkCacheBytes = 0;
  hotRuntimeInfo = null;
}

export function getHotIndex(key: string): BinaryShardIndex | null {
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

export function rememberHotIndex(key: string, index: BinaryShardIndex): void {
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

export function getHotChunk(key: string): string | null {
  const entry = hotChunkCache.get(key);
  if (!entry) {
    return null;
  }

  if (entry.expiresAt <= Date.now()) {
    hotChunkCache.delete(key);
    hotChunkCacheBytes -= entry.size;
    return null;
  }

  hotChunkCache.delete(key);
  hotChunkCache.set(key, entry);
  return entry.value;
}

export function rememberHotChunk(key: string, value: string): void {
  const size = value.length;
  const existing = hotChunkCache.get(key);
  if (existing) {
    hotChunkCacheBytes -= existing.size;
    hotChunkCache.delete(key);
  }

  hotChunkCache.set(key, {
    value,
    size,
    expiresAt: Date.now() + HOT_CHUNK_CACHE_TTL_MS,
  });
  hotChunkCacheBytes += size;

  while (hotChunkCache.size > HOT_CHUNK_CACHE_MAX_ENTRIES || hotChunkCacheBytes > HOT_CHUNK_CACHE_MAX_BYTES) {
    const oldestKey = hotChunkCache.keys().next().value;
    if (oldestKey == null) {
      break;
    }

    const oldest = hotChunkCache.get(oldestKey);
    hotChunkCache.delete(oldestKey);
    if (oldest) {
      hotChunkCacheBytes -= oldest.size;
    }
  }
}

export function getHotRuntimeInfo(): RuntimeInfo | null {
  if (!hotRuntimeInfo) {
    return null;
  }

  if (hotRuntimeInfo.expiresAt <= Date.now()) {
    hotRuntimeInfo = null;
    return null;
  }

  return hotRuntimeInfo.value;
}

export function rememberHotRuntimeInfo(value: RuntimeInfo): void {
  hotRuntimeInfo = {
    value,
    expiresAt: Date.now() + HOT_RUNTIME_INFO_TTL_MS,
  };
}

