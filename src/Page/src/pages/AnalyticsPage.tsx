import { CodeBlock } from '../components/CodeBlock';
import { SectionHeading } from '../components/SectionHeading';

const BIGQUERY_RECEITA_TABLE = 'opencnpj-bigquery.public.receita';
const BIGQUERY_CNO_TABLE = 'opencnpj-bigquery.public.cno';
const BIGQUERY_RNTRC_TABLE = 'opencnpj-bigquery.public.rntrc';
const BIGQUERY_RECEITA_SQL_TABLE = `\`${BIGQUERY_RECEITA_TABLE}\``;

export function AnalyticsPage() {
  return (
    <article className="doc-page">
      <SectionHeading
        level={1}
        title="Consultas Analíticas"
      />

      <p>
        A API é indicada para consulta pontual por CNPJ. Para análises exploratórias, enriquecimento batch e relatórios,
        use as tabelas públicas do OpenCNPJ no BigQuery.
      </p>

      <ul>
        <li><code>{BIGQUERY_RECEITA_TABLE}</code> para Receita Federal.</li>
        <li><code>{BIGQUERY_CNO_TABLE}</code> para Cadastro Nacional de Obras.</li>
        <li><code>{BIGQUERY_RNTRC_TABLE}</code> para RNTRC.</li>
      </ul>

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
FROM ${BIGQUERY_RECEITA_SQL_TABLE}
WHERE cnpj = '12ABC34501DE35'
LIMIT 1;`} />

      <h2>Empresas ativas por UF</h2>
      <CodeBlock language="sql" code={`SELECT
  uf,
  COUNT(*) AS total_empresas
FROM ${BIGQUERY_RECEITA_SQL_TABLE}
WHERE situacao_cadastral = 'Ativa'
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
  receita.razao_social,
  receita.situacao_cadastral,
  receita.uf,
  receita.municipio
FROM minha_base AS base
LEFT JOIN ${BIGQUERY_RECEITA_SQL_TABLE} AS receita
  ON receita.cnpj = base.cnpj;`} />

      <h2>Boas práticas</h2>
      <ul>
        <li>Filtre colunas explicitamente em vez de usar <code>SELECT *</code>.</li>
        <li>Normalize CNPJs antes do join: 14 caracteres, sem máscara, letras em maiúsculas.</li>
        <li>Use a API para detalhes pontuais e o BigQuery para varreduras e agregações.</li>
      </ul>
    </article>
  );
}
