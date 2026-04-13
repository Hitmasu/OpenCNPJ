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
  shard_path_template?: string;
};

export type NdjsonShardLine = {
  cnpj: string;
  data: Record<string, unknown>;
};

