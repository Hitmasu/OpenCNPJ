export const JSON_HEADERS = {
  "Content-Type": "application/json; charset=utf-8",
  "Access-Control-Allow-Origin": "*",
};

export const NO_STORE_HEADERS = {
  ...JSON_HEADERS,
  "Cache-Control": "no-store",
};

export const CACHE_TTL_SECONDS = 60 * 60 * 24;
export const HOT_INDEX_CACHE_TTL_MS = 30 * 60 * 1000;
export const HOT_INDEX_CACHE_MAX_ENTRIES = 256;
export const HOT_CHUNK_CACHE_TTL_MS = 10 * 60 * 1000;
export const HOT_CHUNK_CACHE_MAX_ENTRIES = 32;
export const HOT_CHUNK_CACHE_MAX_BYTES = 8 * 1024 * 1024;
export const HOT_RUNTIME_INFO_TTL_MS = 60 * 1000;
export const SHARD_PREFIX_LENGTH = 3;
export const CNPJ_LENGTH = 14;
export const INDEX_MAGIC = "OCI1";
export const INDEX_HEADER_SIZE = 8;
export const INDEX_ENTRY_SIZE = 26;
export const INDEX_OFFSET_OFFSET = CNPJ_LENGTH;
export const INDEX_LENGTH_OFFSET = CNPJ_LENGTH + 8;
export const R2_PUBLIC_ROOT = "files";
export const CNPJ_MASK_CHARACTERS = /[./-]/g;
export const ALPHANUMERIC_CNPJ_PATTERN = /^[A-Z0-9]{12}\d{2}$/;
