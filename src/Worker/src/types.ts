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
  datasets?: Record<string, DatasetInfo>;
};

export type DatasetInfo = {
  json_property_name?: string;
  storage_release_id?: string;
  zip_available?: boolean;
  zip_size?: number;
  zip_url?: string;
  zip_md5checksum?: string;
};

export type DatasetSelection = {
  includeReceita: boolean;
  moduleKeys: string[];
  cacheKey: string;
};
