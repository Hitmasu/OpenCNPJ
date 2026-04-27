import { CodeBlock } from '../components/CodeBlock';
import { ScalarReferenceRail } from '../components/ScalarReferenceRail';
import { SectionHeading } from '../components/SectionHeading';

export function ApiPage() {
  return (
    <div className="doc-with-rail">
      <article className="doc-page">
        <SectionHeading
          level={1}
          title="API"
        />

        <section className="doc-section">
          <h2>Consulta</h2>
          <CodeBlock language="http" code="GET https://api.opencnpj.org/{CNPJ}" />

          <p><strong>Path</strong> <code>{'{CNPJ}'}</code> com 14 caracteres. Formatos aceitos:</p>
          <ul>
            <li>Numérico sem máscara: <code>60701190000104</code></li>
            <li>Numérico com pontuação completa: <code>60.701.190/0001-04</code></li>
            <li>Alfanumérico sem máscara: <code>12ABC34501DE35</code></li>
            <li>Alfanumérico com pontuação completa: <code>12.ABC.345/01DE-35</code></li>
          </ul>
        </section>

        <section className="doc-section">
          <h2>Filtro de dataset</h2>
          <p>
            Use <code>dataset</code> para escolher a base desejada na consulta. Os valores aceitos são{' '}
            <code>receita</code>, <code>cno</code> e <code>rntrc</code>. Sem filtro, o retorno padrão documentado é o
            dataset da Receita Federal. Nos exemplos, <code>dataset=receita</code> fica explícito para deixar claro o
            formato do parâmetro.
          </p>
          <CodeBlock language="http" code={`GET https://api.opencnpj.org/12ABC34501DE35?dataset=receita
GET https://api.opencnpj.org/12ABC34501DE35?dataset=cno
GET https://api.opencnpj.org/12ABC34501DE35?dataset=rntrc`} />
        </section>

        <section className="doc-section">
          <h2>Contrato</h2>
          <ul>
            <li><strong>Schema:</strong> <a href="https://api.opencnpj.org/schema" target="_blank" rel="noopener">https://api.opencnpj.org/schema</a>.</li>
            <li><strong>Autenticação:</strong> não requer chaves ou tokens.</li>
            <li><strong>Status:</strong> <code>200</code> encontrado, <code>404</code> não encontrado.</li>
          </ul>
        </section>
      </article>

      <aside className="right-rail" aria-label="Executar API">
        <ScalarReferenceRail
          title="Executar chamada"
        />
      </aside>
    </div>
  );
}
