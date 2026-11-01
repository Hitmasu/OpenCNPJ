namespace CNPJExporter.Modules.Receita.Processors;

/// <summary>
/// Expressões compartilhadas pelas projeções JSON (API/ZIP) e colunar (BigQuery).
/// Exigem o CTE de <see cref="LgpdPolicy.BuildCte"/> e o <see cref="Join"/>.
/// </summary>
internal static class LgpdProjection
{
    public const string Join = "LEFT JOIN lgpd ON lgpd.cnpj = UPPER(e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv)";

    public const string Flag = "(lgpd.cnpj IS NOT NULL)";

    // MEI (natureza 2135): a Receita publica o CPF do titular no fim da razão social. Removido para toda a base.
    private const string RazaoSocialSemCpf =
        @"CASE WHEN emp.natureza_juridica = '2135' THEN regexp_replace(COALESCE(emp.razao_social, ''), '\s*[0-9]{11}\s*$', '') ELSE COALESCE(emp.razao_social, '') END";

    public static readonly string RazaoSocial = Blank("razao_social", RazaoSocialSemCpf);

    public static readonly string NomeFantasia = Blank("nome_fantasia", "COALESCE(e.nome_fantasia, '')");

    public static readonly string Email = Blank("email", "COALESCE(e.correio_eletronico, '')");

    // O ELSE define o tipo da coluna: a lista vazia continua LIST(STRUCT) no JSON e no BigQuery.
    public const string Telefones = """
        CASE WHEN list_contains(lgpd.campos, 'telefones') THEN [] ELSE list_filter([
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
        ], telefone -> telefone IS NOT NULL) END
        """;

    // NULL em vez de []: as projeções já convertem QSA ausente em lista vazia, mantendo o tipo da coluna.
    public const string QsaData = "CASE WHEN list_contains(lgpd.campos, 'qsa') THEN NULL ELSE sd.qsa_data END";

    public static string Endereco(string column) => Blank("endereco", $"COALESCE({column}, '')");

    private static string Blank(string field, string value) =>
        $"CASE WHEN list_contains(lgpd.campos, '{field}') THEN '' ELSE {value} END";
}
