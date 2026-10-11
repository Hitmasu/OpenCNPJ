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

    public static readonly string RazaoSocial =
        "CASE WHEN list_contains(lgpd.campos, 'razao_social') THEN " + MaskNome(RazaoSocialSemCpf) + " ELSE " + RazaoSocialSemCpf + " END";

    public static readonly string NomeFantasia =
        "CASE WHEN list_contains(lgpd.campos, 'nome_fantasia') THEN " + MaskNome("COALESCE(e.nome_fantasia, '')") + " ELSE COALESCE(e.nome_fantasia, '') END";

    public const string Logradouro = "CASE WHEN list_contains(lgpd.campos, 'endereco') THEN '' ELSE COALESCE(e.logradouro, '') END";

    public const string Numero = "CASE WHEN list_contains(lgpd.campos, 'endereco') THEN '' ELSE COALESCE(e.numero, '') END";

    public const string Cep = "CASE WHEN list_contains(lgpd.campos, 'endereco') THEN '' ELSE COALESCE(e.cep, '') END";

    // FULANO@EXAMPLE.COM -> F*******@EXAMPLE.COM
    public const string Email =
        @"CASE WHEN list_contains(lgpd.campos, 'email') THEN regexp_replace(COALESCE(e.correio_eletronico, ''), '^([^@])[^@]*', '\1*******') ELSE COALESCE(e.correio_eletronico, '') END";

    // Campo numérico: 98765432 -> 90000000 (DDD preservado).
    public static string TelefoneNumero(string column) =>
        $"CASE WHEN list_contains(lgpd.campos, 'telefones') THEN left(COALESCE({column}, ''), 1) || regexp_replace(substr(COALESCE({column}, ''), 2), '.', '0', 'g') ELSE COALESCE({column}, '') END";

    // FULANO BELTRANO DA SILVA -> FULANO B******
    private static string MaskNome(string expression) =>
        @"regexp_replace(trim(" + expression + @"), '^(\S+)\s+(\S).*$', '\1 \2******')";
}
