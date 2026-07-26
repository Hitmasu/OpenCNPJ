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

export type SegmentRoutingReference = {
  segmentId: string;
  offset: number;
  length: number;
};

export type SegmentRoutingIndex = {
  recordCount: number;
  bytes: Uint8Array;
  entries: Map<string, SegmentRoutingReference[]>;
};

export type RuntimeInfo = {
  storage_release_id?: string;
  datasets?: Record<string, DatasetInfo>;
};

export type DatasetSegmentInfo = {
  id: string;
  storage_release_id: string;
  updated_at?: string;
  record_count?: number;
};

export type DatasetInfo = {
  json_property_name?: string;
  storage_release_id?: string;
  routing_release_id?: string;
  segment_collection_property?: string;
  segments?: DatasetSegmentInfo[];
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
