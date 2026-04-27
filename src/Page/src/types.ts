export type DatasetKey = 'receita' | 'cno' | 'rntrc';

export interface DatasetSchemaField {
  field: string;
  type: string;
  description: string;
}

export interface DatasetDetail {
  name: string;
  shortName: string;
  frequency: string;
  source: string;
  sourceUrl: string;
  sourceDescription: string;
  schemaVersion: string;
  filter: `dataset=${DatasetKey}`;
  description: string;
  schemaFields: DatasetSchemaField[];
}

export interface PublishedDataset {
  updated_at?: string;
  record_count?: number;
  zip_url?: string;
  zip_size?: number | string;
  zip_md5checksum?: string;
}

export interface PublishedInfo {
  total?: number;
  last_updated?: string;
  datasets?: Partial<Record<DatasetKey, PublishedDataset>>;
}
