import { DatasetInfoPanel } from '../components/DatasetInfoPanel';
import { ScalarReferenceRail } from '../components/ScalarReferenceRail';
import { SectionHeading } from '../components/SectionHeading';
import { datasetDetails } from '../data/datasets';
import type { DatasetKey, PublishedInfo } from '../types';

interface DatasetPageProps {
  datasetKey: DatasetKey;
  info: PublishedInfo | null;
  isLoading: boolean;
}

export function DatasetPage({ datasetKey, info, isLoading }: DatasetPageProps) {
  const dataset = datasetDetails[datasetKey];
  const published = info?.datasets?.[datasetKey];

  return (
    <div className="doc-with-rail">
      <article className="doc-page">
        <SectionHeading
          level={1}
          title={dataset.name}
        />

        <section className="doc-section">
          <h2>Sobre o dataset</h2>
          <p>{dataset.description}</p>
          <p>{dataset.sourceDescription}</p>
        </section>

        <DatasetInfoPanel className="doc-section" dataset={dataset} published={published} isLoading={isLoading} />

        <section className="doc-section">
          <h2>Schema dos dados</h2>
          <p>Schema interno do dataset: versão {dataset.schemaVersion}. O contrato consolidado da API fica em <a href="https://api.opencnpj.org/schema" target="_blank" rel="noopener">/schema</a>.</p>
          <div className="table-scroll">
            <table className="lib-table schema-table" aria-label={`Schema do dataset ${dataset.shortName}`}>
              <thead>
                <tr>
                  <th>Campo</th>
                  <th>Tipo</th>
                  <th>Descrição</th>
                </tr>
              </thead>
              <tbody>
                {dataset.schemaFields.map((field) => (
                  <tr key={field.field}>
                    <td><code>{field.field}</code></td>
                    <td>{field.type}</td>
                    <td>{field.description}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </article>

      <aside className="right-rail" aria-label={`Executar API do dataset ${dataset.shortName}`}>
        <ScalarReferenceRail
          key={datasetKey}
          title={`Consultar ${dataset.shortName}`}
          datasetKey={datasetKey}
        />
      </aside>
    </div>
  );
}
