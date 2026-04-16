namespace CNPJExporter.Modules.Receita.Processors;

internal static class QsaProjection
{
    public static string BuildCte(string cteName, string sourceRelation, string? prefixLiteral = null)
    {
        var where = string.IsNullOrWhiteSpace(prefixLiteral)
            ? ""
            : $"WHERE s.cnpj_prefix = '{prefixLiteral}'";

        return $@"{cteName} AS (
                SELECT 
                    s.cnpj_prefix,
                    s.cnpj_basico,
                    array_agg(struct_pack(
                        nome_socio := COALESCE(s.nome_socio, ''),
                        cnpj_cpf_socio := COALESCE(s.cnpj_cpf_socio, ''),
                        qualificacao_socio := COALESCE(qs.descricao, ''),
                        data_entrada_sociedade := CASE 
                            WHEN s.data_entrada_sociedade ~ '^[0-9]{{8}}$' 
                            THEN SUBSTRING(s.data_entrada_sociedade, 1, 4) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 5, 2) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 7, 2)
                            ELSE COALESCE(s.data_entrada_sociedade, '')
                        END,
                        identificador_socio := CASE s.identificador_socio
                            WHEN '1' THEN 'Pessoa Jurídica'
                            WHEN '2' THEN 'Pessoa Física'
                            WHEN '3' THEN 'Estrangeiro'
                            ELSE COALESCE(s.identificador_socio, '')
                        END,
                        faixa_etaria := CASE s.faixa_etaria
                            WHEN '0' THEN 'Não se aplica'
                            WHEN '1' THEN '0 a 12 anos'
                            WHEN '2' THEN '13 a 20 anos'
                            WHEN '3' THEN '21 a 30 anos'
                            WHEN '4' THEN '31 a 40 anos'
                            WHEN '5' THEN '41 a 50 anos'
                            WHEN '6' THEN '51 a 60 anos'
                            WHEN '7' THEN '61 a 70 anos'
                            WHEN '8' THEN '71 a 80 anos'
                            WHEN '9' THEN 'Mais de 80 anos'
                            ELSE COALESCE(s.faixa_etaria, '')
                        END
                    )) as qsa_data
                FROM {sourceRelation} s
                LEFT JOIN qualificacao qs ON s.qualificacao_socio = qs.codigo
                {where}
                GROUP BY s.cnpj_prefix, s.cnpj_basico
            )";
    }
}
