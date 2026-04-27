interface NavigationItem {
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
    ],
  },
  { href: '#/consultas-analiticas', label: 'Consultas analíticas' },
  { href: '#/bibliotecas', label: 'Bibliotecas' },
  { href: '#/faq', label: 'Perguntas frequentes' },
];
