import { CodeBlock } from '../components/CodeBlock';
import { SectionHeading } from '../components/SectionHeading';

const BIGQUERY_TABLE = 'opencnpj-bigquery.public.empresas';
const BIGQUERY_SQL_TABLE = `\`${BIGQUERY_TABLE}\``;

export function AnalyticsPage() {
  return (
    <article className="doc-page">
      <SectionHeading
        level={1}
        title="Consultas Analíticas"
      />

      <p>
        A API é indicada para consulta pontual por CNPJ. Para análises exploratórias, enriquecimento batch e relatórios,
        use a tabela pública do OpenCNPJ no BigQuery: <code>{BIGQUERY_TABLE}</code>.
      </p>

      <div className="actions">
        <a className="btn primary" href="https://bigquery.opencnpj.org" target="_blank" rel="noopener">Abrir no BigQuery</a>
        <a className="btn" href="#/datasets/receita">Ver Receita Federal</a>
      </div>

      <h2>Quando usar</h2>
      <ul>
        <li>Contar empresas por UF, município, CNAE, situação cadastral ou porte.</li>
        <li>Validar uma lista de CNPJs com joins em massa.</li>
        <li>Gerar bases internas de enriquecimento sem consultar a API CNPJ por CNPJ.</li>
        <li>Executar auditorias e recortes históricos do release publicado.</li>
      </ul>

      <h2>Consulta por CNPJ</h2>
      <CodeBlock language="sql" code={`SELECT
  cnpj,
  razao_social,
  nome_fantasia,
  situacao_cadastral,
  uf,
  municipio,
  cnae_principal
FROM ${BIGQUERY_SQL_TABLE}
WHERE cnpj = '12ABC34501DE35'
LIMIT 1;`} />

      <h2>Empresas ativas por UF</h2>
      <CodeBlock language="sql" code={`SELECT
  uf,
  COUNT(*) AS total_empresas
FROM ${BIGQUERY_SQL_TABLE}
WHERE situacao_cadastral = 'ATIVA'
GROUP BY uf
ORDER BY total_empresas DESC;`} />

      <h2>Join com sua lista</h2>
      <p>
        Para validar muitos CNPJs, envie sua lista para uma tabela temporária ou permanente e faça join pelo CNPJ sem máscara.
      </p>
      <CodeBlock language="sql" code={`WITH minha_base AS (
  SELECT '12ABC34501DE35' AS cnpj UNION ALL
  SELECT '60701190000104' AS cnpj
)
SELECT
  base.cnpj,
  empresas.razao_social,
  empresas.situacao_cadastral,
  empresas.uf,
  empresas.municipio
FROM minha_base AS base
LEFT JOIN ${BIGQUERY_SQL_TABLE} AS empresas
  ON empresas.cnpj = base.cnpj;`} />

      <h2>Boas práticas</h2>
      <ul>
        <li>Filtre colunas explicitamente em vez de usar <code>SELECT *</code>.</li>
        <li>Normalize CNPJs antes do join: 14 caracteres, sem máscara, letras em maiúsculas.</li>
        <li>Use a API para detalhes pontuais e o BigQuery para varreduras e agregações.</li>
      </ul>
    </article>
  );
}
