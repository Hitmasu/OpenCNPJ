export interface NavigationItem {
  href?: `#/${string}`;
  label: string;
  children?: NavigationItem[];
}

export const navigationItems: NavigationItem[] = [
  { href: '#/sobre', label: 'Sobre o projeto' },
  { href: '#/api', label: 'API' },
  { href: '#/limites', label: 'Limites' },
  {
    label: 'Datasets',
    children: [
      { href: '#/datasets/receita', label: 'Receita Federal' },
      { href: '#/datasets/cno', label: 'CNO' },
      { href: '#/datasets/rntrc', label: 'RNTRC' },
      {
        label: 'Portal da Transparência',
        children: [
          { href: '#/datasets/favorecidos_pj', label: 'Favorecidos PJ' },
          { href: '#/datasets/ceis', label: 'CEIS' },
          { href: '#/datasets/cepim', label: 'CEPIM' },
          { href: '#/datasets/cnep', label: 'CNEP' },
          { href: '#/datasets/acordos_leniencia', label: 'Acordos de leniência' },
          { href: '#/datasets/licitacoes', label: 'Licitações' },
          { href: '#/datasets/contratos', label: 'Contratos' },
          { href: '#/datasets/renuncias_fiscais', label: 'Renúncias fiscais' },
          { href: '#/datasets/notas_fiscais', label: 'Notas fiscais' },
          { href: '#/datasets/convenios', label: 'Convênios' },
          { href: '#/datasets/emendas_parlamentares', label: 'Emendas parlamentares' },
          { href: '#/datasets/emendas_documentos', label: 'Documentos de emendas' },
        ],
      },
    ],
  },
  { href: '#/consultas-analiticas', label: 'Consultas analíticas' },
  { href: '#/bibliotecas', label: 'Bibliotecas' },
  { href: '#/faq', label: 'Perguntas frequentes' },
];
