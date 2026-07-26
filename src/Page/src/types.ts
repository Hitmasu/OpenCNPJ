export type DatasetKey =
  | 'receita'
  | 'cno'
  | 'rntrc'
  | 'favorecidos_pj'
  | 'ceis'
  | 'cepim'
  | 'cnep'
  | 'acordos_leniencia'
  | 'licitacoes'
  | 'contratos'
  | 'renuncias_fiscais'
  | 'notas_fiscais'
  | 'convenios'
  | 'emendas_parlamentares'
  | 'emendas_documentos';

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
  filter: `datasets=${DatasetKey}`;
  description: string;
  schemaFields: DatasetSchemaField[];
}

export interface PublishedDataset {
  updated_at?: string;
  record_count?: number;
  zip_url?: string;
  zip_size?: number | string;
  zip_md5checksum?: string;
  segments?: PublishedDatasetSegment[] | null;
}

export interface PublishedDatasetSegment {
  id: string;
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
