import { SectionHeading } from '../components/SectionHeading';

const faqItems = [
  {
    question: 'Posso usar o serviço gratuitamente na minha empresa ou serviço?',
    answer: <p>Sim. O uso é gratuito, inclusive para fins comerciais. Não oferecemos suporte dedicado.</p>,
  },
  {
    question: 'Os dados são consultados ou atualizados em tempo real?',
    answer: (
      <p>
        Não. As bases são derivadas de fontes públicas e publicadas em ciclos. Consulte as páginas da{' '}
        <a href="#/datasets/receita">Receita Federal</a>, <a href="#/datasets/cno">CNO</a> e{' '}
        <a href="#/datasets/rntrc">RNTRC</a> para ver a última atualização de cada base.
      </p>
    ),
  },
  {
    question: 'Posso consultar massivamente os dados?',
    answer: (
      <>
        <p>A API é indicada para consultas pontuais por CNPJ. Não há uma cota pública fixa para uso normal, mas picos muito altos e constantes podem acionar proteção temporária com retorno <code>429</code>.</p>
        <p>Para validação de bases, enriquecimento em lote ou consultas analíticas em larga escala, use o dataset público no <a href="#/consultas-analiticas">BigQuery</a> ou os downloads NDJSON publicados por base.</p>
      </>
    ),
  },
  {
    question: 'Minha empresa tem muitas chamadas por segundo. Vocês oferecem plano pago?',
    answer: <p>Não. Todos os serviços são gratuitos e não há plano pago para aumento de limite. Para picos de API, consulte apenas o dataset necessário; para volumes massivos, prefira BigQuery.</p>,
  },
  {
    question: 'Existe limite diário ou mensal de consultas?',
    answer: <p>Não publicamos uma cota diária ou mensal para consultas pontuais. A proteção é aplicada em casos de volume muito alto e constante; varreduras, agregações e listas grandes devem ir para BigQuery.</p>,
  },
  {
    question: 'Vocês armazenam logs das consultas?',
    answer: <p>Não armazenamos logs identificáveis das consultas. Mantemos apenas métricas agregadas, como quantidade de consultas e tempos de resposta, para estatísticas e melhoria contínua.</p>,
  },
  {
    question: 'Consultei um CNPJ e não foi encontrado, mas ele existe. Por quê?',
    answer: <p>Se o CNPJ foi aberto recentemente, ele pode ainda não constar na base publicada pelo OpenCNPJ. Se você usou filtro de dataset, verifique também se a base escolhida possui registros para esse CNPJ.</p>,
  },
];

export function FaqPage() {
  return (
    <article className="doc-page">
      <SectionHeading
        level={1}
        title="Perguntas frequentes"
      />

      <div className="faq">
        {faqItems.map((item) => (
          <section className="faq-item" key={item.question}>
            <h2>{item.question}</h2>
            <div className="answer">{item.answer}</div>
          </section>
        ))}
      </div>
    </article>
  );
}
