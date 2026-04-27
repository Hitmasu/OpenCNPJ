import type { DatasetDetail, PublishedDataset } from '../types';
import { formatBytes, formatCount, formatDate } from '../utils/format';
import { DownloadIcon } from './Icons';

interface DatasetInfoPanelProps {
  dataset: DatasetDetail;
  published?: PublishedDataset;
  isLoading?: boolean;
  className?: string;
}

function publicationValue(isLoading: boolean, published: PublishedDataset | undefined, value: string) {
  if (isLoading) return 'Carregando...';
  if (!published) return 'Não publicado no /info atual';
  return value;
}

export function DatasetInfoPanel({
  dataset,
  published,
  isLoading = false,
  className,
}: DatasetInfoPanelProps) {
  const classes = ['dataset-info-panel', className].filter(Boolean).join(' ');

  return (
    <section className={classes} aria-label={`Informações do dataset ${dataset.shortName}`}>
      <h2>Informações do dataset</h2>
      <dl className="dataset-info-list">
        <div>
          <dt>Origem</dt>
          <dd><a href={dataset.sourceUrl} target="_blank" rel="noopener">{dataset.source}</a></dd>
        </div>
        <div>
          <dt>Frequência</dt>
          <dd>{dataset.frequency}</dd>
        </div>
        <div>
          <dt>Filtro</dt>
          <dd><code>{dataset.filter}</code></dd>
        </div>
        <div>
          <dt>Schema</dt>
          <dd>Versão {dataset.schemaVersion}</dd>
        </div>
        <div>
          <dt>Última publicação</dt>
          <dd>{publicationValue(isLoading, published, formatDate(published?.updated_at))}</dd>
        </div>
        <div>
          <dt>Registros</dt>
          <dd>{publicationValue(isLoading, published, formatCount(published?.record_count))}</dd>
        </div>
        <div className="dataset-info-download">
          <dt>Download</dt>
          <dd>
            {published?.zip_url ? (
              <dl className="dataset-download-list">
                <div>
                  <dt>Arquivo</dt>
                  <dd>
                    <a className="btn primary" href={published.zip_url} target="_blank" rel="noopener" download="" aria-label={`Baixar dataset ${dataset.name}`}>
                      <DownloadIcon />
                      Baixar dataset
                    </a>
                  </dd>
                </div>
                <div>
                  <dt>Tamanho</dt>
                  <dd>{formatBytes(published.zip_size)}</dd>
                </div>
                <div>
                  <dt>MD5</dt>
                  <dd className="dataset-download-checksum">{published.zip_md5checksum || '-'}</dd>
                </div>
              </dl>
            ) : (
              <span>{isLoading ? 'Carregando...' : 'Download indisponível no momento.'}</span>
            )}
          </dd>
        </div>
      </dl>
    </section>
  );
}
