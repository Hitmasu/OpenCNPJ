namespace CNPJExporter.Modules.Receita.Processors;

internal static class JsonProjection
{
    public const string Fields = @"cnpj := e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv,
                    razao_social := COALESCE(emp.razao_social, ''),
                    nome_fantasia := COALESCE(e.nome_fantasia, ''),
                    situacao_cadastral := CASE LPAD(e.situacao_cadastral, 2, '0')
                        WHEN '01' THEN 'Nula'
                        WHEN '02' THEN 'Ativa'
                        WHEN '03' THEN 'Suspensa'
                        WHEN '04' THEN 'Inapta'
                        WHEN '08' THEN 'Baixada'
                        ELSE e.situacao_cadastral
                    END,
                    data_situacao_cadastral := CASE 
                        WHEN e.data_situacao_cadastral ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(e.data_situacao_cadastral, 1, 4) || '-' || 
                             SUBSTRING(e.data_situacao_cadastral, 5, 2) || '-' || 
                             SUBSTRING(e.data_situacao_cadastral, 7, 2)
                        ELSE COALESCE(e.data_situacao_cadastral, '')
                    END,
                    matriz_filial := CASE e.identificador_matriz_filial
                        WHEN '1' THEN 'Matriz'
                        WHEN '2' THEN 'Filial'
                        ELSE e.identificador_matriz_filial
                    END,
                    data_inicio_atividade := CASE 
                        WHEN e.data_inicio_atividade ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(e.data_inicio_atividade, 1, 4) || '-' || 
                             SUBSTRING(e.data_inicio_atividade, 5, 2) || '-' || 
                             SUBSTRING(e.data_inicio_atividade, 7, 2)
                        ELSE COALESCE(e.data_inicio_atividade, '')
                    END,
                    cnae_principal := COALESCE(e.cnae_principal, ''),
                    cnaes_secundarios := CASE 
                        WHEN e.cnaes_secundarios IS NOT NULL AND e.cnaes_secundarios != ''
                        THEN string_split(e.cnaes_secundarios, ',')
                        ELSE []
                    END,
                    natureza_juridica := COALESCE(nat.descricao, ''),
                    tipo_logradouro := COALESCE(e.tipo_logradouro, ''),
                    logradouro := COALESCE(e.logradouro, ''),
                    numero := COALESCE(e.numero, ''),
                    complemento := COALESCE(e.complemento, ''),
                    bairro := COALESCE(e.bairro, ''),
                    cep := COALESCE(e.cep, ''),
                    uf := COALESCE(e.uf, ''),
                    municipio := COALESCE(mun.descricao, ''),
                    email := COALESCE(e.correio_eletronico, ''),
                    telefones := list_filter([
                        CASE WHEN e.ddd1 IS NOT NULL OR e.telefone1 IS NOT NULL
                             THEN struct_pack(ddd := COALESCE(e.ddd1, ''), numero := COALESCE(e.telefone1, ''), is_fax := false)
                             ELSE NULL
                        END,
                        CASE WHEN e.ddd2 IS NOT NULL OR e.telefone2 IS NOT NULL  
                             THEN struct_pack(ddd := COALESCE(e.ddd2, ''), numero := COALESCE(e.telefone2, ''), is_fax := false)
                             ELSE NULL
                        END,
                        CASE WHEN e.ddd_fax IS NOT NULL OR e.fax IS NOT NULL
                             THEN struct_pack(ddd := COALESCE(e.ddd_fax, ''), numero := COALESCE(e.fax, ''), is_fax := true)
                             ELSE NULL
                        END
                    ], x -> x IS NOT NULL),
                    capital_social := COALESCE(emp.capital_social, ''),
                    porte_empresa := CASE emp.porte_empresa
                        WHEN '00' THEN 'Não informado'
                        WHEN '01' THEN 'Microempresa (ME)'
                        WHEN '03' THEN 'Empresa de Pequeno Porte (EPP)'
                        WHEN '05' THEN 'Demais'
                        ELSE COALESCE(emp.porte_empresa, '')
                    END,
                    opcao_simples := COALESCE(s.opcao_simples, ''),
                    data_opcao_simples := CASE 
                        WHEN s.data_opcao_simples ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(s.data_opcao_simples, 1, 4) || '-' || 
                             SUBSTRING(s.data_opcao_simples, 5, 2) || '-' || 
                             SUBSTRING(s.data_opcao_simples, 7, 2)
                        ELSE COALESCE(s.data_opcao_simples, '')
                    END,
                    opcao_mei := COALESCE(s.opcao_mei, ''),
                    data_opcao_mei := CASE 
                        WHEN s.data_opcao_mei ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(s.data_opcao_mei, 1, 4) || '-' || 
                             SUBSTRING(s.data_opcao_mei, 5, 2) || '-' || 
                             SUBSTRING(s.data_opcao_mei, 7, 2)
                        ELSE COALESCE(s.data_opcao_mei, '')
                    END,
                    QSA := COALESCE(sd.qsa_data, [])";
}
