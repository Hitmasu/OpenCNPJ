import { GitHubIcon } from '../components/Icons';
import { SectionHeading } from '../components/SectionHeading';

function reverse(value: string) {
  return value.split('').reverse().join('');
}

function buildContactAddress() {
  const mailbox = reverse('otatnoc');
  const domain = reverse('gro.jpncnepo');

  return `${mailbox}@${domain}`;
}

export function AboutPage() {
  function handleContactClick() {
    window.location.href = `mailto:${buildContactAddress()}`;
  }

  return (
    <article className="doc-page">
      <SectionHeading
        level={1}
        title="OpenCNPJ"
      />

      <section className="doc-section">
        <p className="about-lead">
          O OpenCNPJ transforma bases públicas de empresas brasileiras em uma API gratuita, sem token e sem cadastro.
        </p>
        <p>
          A ideia é simples: reduzir o trabalho de quem precisa validar CNPJs, enriquecer cadastros ou consultar dados
          empresariais publicados, sem pagar por APIs que empacotam dados públicos e sem manter uma esteira própria de
          download, parsing e publicação.
        </p>
        <div className="actions">
          <a className="btn primary" href="#/api">Ver API</a>
          <a className="btn" href="#/datasets/receita">Explorar datasets</a>
          <a className="btn" href="#/consultas-analiticas">Consultas analíticas</a>
        </div>
      </section>

      <section className="doc-section">
        <h2>Para que serve</h2>
        <ul>
          <li>Consultar um CNPJ específico em integrações, cadastros e backoffices.</li>
          <li>Enriquecer registros internos com dados cadastrais públicos.</li>
          <li>Baixar datasets separados quando a integração precisa processar arquivos próprios.</li>
          <li>Usar BigQuery quando a pergunta envolve listas, filtros, relatórios ou milhões de empresas.</li>
        </ul>
      </section>

      <section className="doc-section">
        <h2>Como funciona</h2>
        <dl className="summary-list">
          <div>
            <dt>Coleta</dt>
            <dd>O pipeline acompanha fontes públicas, como a base de CNPJ publicada pela Receita Federal.</dd>
          </div>
          <div>
            <dt>Publicação</dt>
            <dd>Os dados são normalizados e publicados em releases estáticos consultados pela API.</dd>
          </div>
          <div>
            <dt>Consumo</dt>
            <dd>Você escolhe entre API, downloads por dataset ou BigQuery conforme o volume.</dd>
          </div>
        </dl>
      </section>

      <section className="doc-section">
        <h2>Escolha o caminho certo</h2>
        <div className="dataset-guide">
          <article>
            <h3>API</h3>
            <p>Ideal para formulários, validações em fluxo de cadastro e enriquecimento sob demanda.</p>
          </article>
          <article>
            <h3>Datasets</h3>
            <p>Ideal para carregar a base na sua própria infraestrutura, com controle total de processamento e cache.</p>
          </article>
          <article>
            <h3>BigQuery</h3>
            <p>Ideal para análises, prospecção de leads, filtros por CNAE e cruzamentos em grande volume.</p>
          </article>
        </div>
      </section>

      <section className="doc-section">
        <h2>Transparência</h2>
        <p>
          O OpenCNPJ é 100% open source e mantido sem fins lucrativos.
        </p>
        <p>
          Para que o projeto continue atualizado, online e com limites generosos em comparação com outras APIs pagas e
          gratuitas, as doações são importantes para ajudar a manter a infraestrutura e a publicação dos dados.
        </p>
        <p>
          Se o OpenCNPJ ajudou sua empresa, acelerou uma integração ou contribuiu para lançar um produto no mercado,
          considere apoiar o projeto para que ele continue disponível para todos.
        </p>
        <p>
          O OpenCNPJ não substitui validação jurídica, consulta oficial em tempo real nem análise de risco. Ele publica
          dados públicos processados em releases.
        </p>
        <div className="actions">
          <a className="btn" href="https://github.com/Hitmasu/opencnpj" target="_blank" rel="noopener" aria-label="Ver OpenCNPJ no GitHub">
            <GitHubIcon />
            Ver no GitHub
          </a>
        </div>
      </section>

      <section className="doc-section">
        <h2>Contato</h2>
        <p>
          Para dúvidas públicas, sugestões, problemas na documentação ou pedidos que podem ajudar outras pessoas, prefira
          abrir uma issue no GitHub. Se o assunto precisar ser tratado diretamente, envie um e-mail para o contato do projeto.
        </p>
        <div className="contact-line">
          <span>contato [arroba] opencnpj [ponto] org</span>
        </div>
        <div className="actions">
          <a className="btn" href="https://github.com/Hitmasu/opencnpj/issues" target="_blank" rel="noopener" aria-label="Abrir issue do OpenCNPJ no GitHub">
            <GitHubIcon />
            Abrir issue
          </a>
          <button className="btn" type="button" onClick={handleContactClick}>Enviar e-mail</button>
        </div>
      </section>
    </article>
  );
}
