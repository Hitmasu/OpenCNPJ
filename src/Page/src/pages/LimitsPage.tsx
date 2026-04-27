import { SectionHeading } from '../components/SectionHeading';

export function LimitsPage() {
  return (
    <article className="doc-page">
      <SectionHeading
        level={1}
        title="Limites"
      />

      <section className="doc-section">
        <h2>Política atual</h2>
        <p>
          O OpenCNPJ não trabalha com uma cota pública fixa para o uso normal da API. O endpoint não exige chave,
          cadastro ou plano pago, e picos temporários, inclusive acima de <strong>100 requisições por segundo</strong>,
          são aceitos quando o padrão de uso continua sendo consulta pontual por CNPJ.
        </p>
        <p>
          A proteção entra quando uma origem mantém um volume muito alto de consultas por um período contínuo. Nesses
          casos, um bloqueio temporário pode ser aplicado para preservar o serviço, e a API retorna <code>429</code>
          até o tráfego normalizar.
        </p>
        <p>
          A diferença principal não é a velocidade da chamada, mas o objetivo da consulta. A API é adequada quando você
          já sabe qual CNPJ deseja consultar. Se você precisa descobrir empresas, montar listas, varrer segmentos,
          cruzar bases grandes ou gerar estatísticas, a ferramenta correta é o BigQuery ou o download dos datasets.
        </p>
      </section>

      <section className="doc-section">
        <h2>Use a API quando</h2>
        <p>
          Pense na API como uma consulta direta: você informa um CNPJ e recebe os dados publicados para aquela empresa.
        </p>
        <ul>
          <li>Você já possui o CNPJ e precisa consultar a situação cadastral ou um dataset adicional.</li>
          <li>Um cadastro, checkout, backoffice ou integração precisa validar uma empresa durante o fluxo do usuário.</li>
          <li>O volume vem de ações pontuais, mesmo que existam picos temporários de tráfego.</li>
          <li>Você quer reduzir payload informando apenas o dataset necessário na consulta.</li>
        </ul>
      </section>

      <section className="doc-section">
        <h2>Use BigQuery quando</h2>
        <p>
          O BigQuery público do OpenCNPJ é indicado quando a pergunta depende de olhar para muitas empresas ao mesmo
          tempo. Ele é gratuito para começar, executa filtros e agregações em segundos e permite consultar milhões de
          registros sem transformar uma análise em milhares ou milhões de chamadas HTTP.
        </p>
        <ul>
          <li>
            <strong>Leads por CNAE:</strong> se você quer encontrar empresas de um setor para prospecção, use o{' '}
            <a href="https://bigquery.opencnpj.org" target="_blank" rel="noopener">BigQuery do OpenCNPJ</a> para filtrar
            por CNAE, UF, município, porte e situação cadastral em uma única consulta.
          </li>
          <li>
            <strong>Relatórios comerciais:</strong> se você precisa contar empresas por região, setor ou porte, faça uma
            agregação no BigQuery em vez de consultar cada CNPJ separadamente.
          </li>
          <li>
            <strong>Enriquecimento de bases:</strong> se você tem uma lista grande de CNPJs, suba a lista para uma tabela
            e faça join pelo CNPJ para obter os campos necessários em lote.
          </li>
          <li>
            <strong>Análises recorrentes:</strong> se a consulta vira rotina de BI, auditoria, segmentação ou monitoramento,
            mantenha essa lógica no BigQuery e use a API apenas para detalhes pontuais.
          </li>
        </ul>
        <div className="actions limits-actions">
          <a className="btn primary" href="https://bigquery.opencnpj.org" target="_blank" rel="noopener">Abrir BigQuery</a>
          <a className="btn" href="#/consultas-analiticas">Ver consultas analíticas</a>
        </div>
      </section>

      <section className="doc-section">
        <h2>Regra prática</h2>
        <p>
          Se a pergunta é sobre um CNPJ específico, use a API. Se a pergunta começa com "quais empresas",
          "quantas empresas", "liste empresas", "filtre empresas" ou "cruze minha base inteira", use BigQuery.
        </p>
      </section>
    </article>
  );
}
