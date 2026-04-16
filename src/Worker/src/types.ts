export interface Env {
  CNPJ_BUCKET: R2Bucket;
  ASSETS?: Fetcher;
}

export type BinaryShardIndex = {
  recordCount: number;
  bytes: Uint8Array;
  view: DataView;
};

export type BinaryIndexEntry = {
  offset: number;
  length: number;
};

export type RuntimeInfo = {
  storage_release_id?: string;
  default_shard_release_id?: string;
  shard_releases?: Record<string, string>;
  module_shards?: Record<string, ModuleShardInfo>;
};

export type ModuleShardInfo = {
  json_property_name?: string;
  storage_release_id?: string;
  default_shard_release_id?: string;
  shard_releases?: Record<string, string>;
};

export type DatasetSelection = {
  includeReceita: boolean;
  moduleKeys: string[];
  cacheKey: string;
};
