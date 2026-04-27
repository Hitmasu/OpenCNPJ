import type { DatasetDetail, DatasetKey } from '../types';

export const datasetOrder: DatasetKey[] = ['receita', 'cno', 'rntrc'];

export const datasetDetails: Record<DatasetKey, DatasetDetail> = {
  receita: {
    name: 'Receita Federal',
    shortName: 'Receita',
    frequency: 'Mensal',
    source: 'Arquivos públicos de CNPJ da Receita Federal',
    sourceUrl: 'https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj',
    sourceDescription:
      'O Cadastro Nacional da Pessoa Jurídica identifica empresas e estabelecimentos no Brasil. Ele reúne dados usados para validar cadastros, conferir situação cadastral, localizar matriz e filiais, entender atividades econômicas, consultar endereço, porte, natureza jurídica, Simples/MEI e quadro societário.',
    schemaVersion: '1',
    filter: 'dataset=receita',
    description:
      'Base cadastral principal do CNPJ: estabelecimento, razão social, nome fantasia, CNAEs, endereço, situação cadastral, Simples/MEI e quadro societário.',
    schemaFields: [
      { field: 'cnpj', type: 'string', description: 'CNPJ com 14 caracteres, sem máscara. Pode conter letras nos 12 primeiros caracteres e 2 dígitos verificadores no final.' },
      { field: 'razao_social', type: 'string', description: 'Razão social da pessoa jurídica.' },
      { field: 'nome_fantasia', type: 'string', description: 'Nome fantasia publicado pela Receita, quando existir.' },
      { field: 'situacao_cadastral', type: 'string', description: 'Situação cadastral mapeada a partir do código da Receita.' },
      { field: 'data_situacao_cadastral', type: 'string', description: 'Data da situação cadastral no formato YYYY-MM-DD.' },
      { field: 'matriz_filial', type: 'string', description: 'Indica matriz ou filial.' },
      { field: 'data_inicio_atividade', type: 'string', description: 'Data de início de atividade no formato YYYY-MM-DD.' },
      { field: 'cnae_principal', type: 'string', description: 'CNAE principal com 7 dígitos.' },
      { field: 'cnaes_secundarios', type: 'string[]', description: 'Lista de CNAEs secundários.' },
      { field: 'natureza_juridica', type: 'string', description: 'Natureza jurídica da empresa.' },
      { field: 'endereco', type: 'campos string', description: 'tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf e municipio.' },
      { field: 'contato', type: 'campos string/array', description: 'email e telefones publicados na base cadastral.' },
      { field: 'capital_social', type: 'string', description: 'Capital social em reais, com vírgula decimal.' },
      { field: 'porte_empresa', type: 'string', description: 'Porte cadastral da empresa.' },
      { field: 'simples_mei', type: 'campos string', description: 'opcao_simples, data_opcao_simples, opcao_mei e data_opcao_mei.' },
      { field: 'QSA', type: 'array', description: 'Quadro societário, com CPF mascarado quando o sócio é pessoa física.' },
    ],
  },
  cno: {
    name: 'Cadastro Nacional de Obras',
    shortName: 'CNO',
    frequency: 'Verificação diária',
    source: 'Cadastro Nacional de Obras da Receita Federal',
    sourceUrl: 'https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-de-obras-cno',
    sourceDescription:
      'O Cadastro Nacional de Obras descreve obras de construção civil e seus responsáveis. Ele ajuda a identificar vínculos entre empresas e obras, acompanhar situação cadastral, localização, área informada, datas relevantes e dados do responsável pela obra.',
    schemaVersion: '2',
    filter: 'dataset=cno',
    description:
      'Obras vinculadas ao CNPJ responsável, incluindo dados de obra, situação, localização, área e vínculos conhecidos.',
    schemaFields: [
      { field: 'cno.updated_at', type: 'string', description: 'Timestamp ISO 8601 da atualização do módulo.' },
      { field: 'cno.obras[].cno', type: 'string', description: 'Identificador da obra no CNO.' },
      { field: 'cno.obras[].nome', type: 'string', description: 'Nome da obra.' },
      { field: 'cno.obras[].nome_empresarial', type: 'string', description: 'Nome empresarial relacionado à obra.' },
      { field: 'cno.obras[].situacao', type: 'object', description: 'Código e descrição da situação da obra.' },
      { field: 'cno.obras[].datas', type: 'campos string', description: 'data_inicio, data_inicio_responsabilidade, data_registro e data_situacao.' },
      { field: 'cno.obras[].endereco', type: 'campos string', description: 'cep, uf, municipio, logradouro, numero, bairro e complemento.' },
      { field: 'cno.obras[].area_total', type: 'string', description: 'Área total conforme unidade de medida da fonte.' },
      { field: 'cno.obras[].qualificacao_responsavel', type: 'object', description: 'Código e descrição da qualificação do responsável.' },
      { field: 'cno.obras[].codigo_localizacao', type: 'string', description: 'Código de localização publicado na fonte.' },
    ],
  },
  rntrc: {
    name: 'Registro Nacional de Transportadores Rodoviários de Cargas',
    shortName: 'RNTRC',
    frequency: 'Verificação diária',
    source: 'Dados Abertos da ANTT',
    sourceUrl: 'https://dados.gov.br/dados/conjuntos-dados/rntrc',
    sourceDescription:
      'O Registro Nacional de Transportadores Rodoviários de Cargas identifica transportadores autorizados a atuar no transporte rodoviário de cargas. Ele é útil para validar fornecedores logísticos, verificar situação do registro, categoria do transportador, município, UF e datas cadastrais.',
    schemaVersion: '1',
    filter: 'dataset=rntrc',
    description:
      'Registro do transportador: número do registro, categoria, situação, município, UF e datas cadastrais.',
    schemaFields: [
      { field: 'rntrc.updated_at', type: 'string', description: 'Timestamp ISO 8601 da atualização do módulo.' },
      { field: 'rntrc.numero_rntrc', type: 'string', description: 'Número do registro RNTRC.' },
      { field: 'rntrc.nome', type: 'string', description: 'Nome do transportador na fonte.' },
      { field: 'rntrc.categoria', type: 'string', description: 'Categoria do transportador, como ETC ou TAC.' },
      { field: 'rntrc.situacao', type: 'string', description: 'Situação do registro na ANTT.' },
      { field: 'rntrc.data_primeiro_cadastro', type: 'string', description: 'Data do primeiro cadastro no RNTRC.' },
      { field: 'rntrc.data_situacao', type: 'string', description: 'Data da situação do registro.' },
      { field: 'rntrc.cep', type: 'string', description: 'CEP informado na fonte.' },
      { field: 'rntrc.municipio', type: 'string', description: 'Município do transportador.' },
      { field: 'rntrc.uf', type: 'string', description: 'UF do transportador.' },
      { field: 'rntrc.equiparado', type: 'boolean', description: 'Indica se o transportador consta como equiparado.' },
    ],
  },
};
