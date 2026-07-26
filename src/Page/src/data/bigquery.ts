import type { DatasetKey } from '../types';

export const BIGQUERY_TABLE_PREFIX = 'opencnpj-bigquery.public';

export function getBigQueryTable(datasetKey: DatasetKey) {
  return `${BIGQUERY_TABLE_PREFIX}.${datasetKey}`;
}
