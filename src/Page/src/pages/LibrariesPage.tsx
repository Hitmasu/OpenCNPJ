import { SectionHeading } from '../components/SectionHeading';
import { libraries } from '../data/libraries';

export function LibrariesPage() {
  return (
    <article className="doc-page">
      <SectionHeading
        level={1}
        title="Bibliotecas"
      />

      <section className="doc-section">
        <p>
          Estas bibliotecas são criadas e mantidas pela comunidade para facilitar a integração com a API do OpenCNPJ em
          diferentes linguagens e frameworks.
        </p>
      </section>

      <div className="table-scroll">
        <table className="lib-table" aria-label="Bibliotecas por linguagem">
          <thead>
            <tr>
              <th>Linguagem</th>
              <th>Criador</th>
              <th>Repositório</th>
            </tr>
          </thead>
          <tbody>
            {libraries.map((library) => (
              <tr key={`${library.language}-${library.href}`}>
                <td>{library.language}</td>
                <td>{library.author}</td>
                <td><a href={library.href} target="_blank" rel="noopener">{library.repository}</a></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="hint">Tem uma biblioteca em outra linguagem? Envie um PR para adicionarmos aqui.</p>
    </article>
  );
}
