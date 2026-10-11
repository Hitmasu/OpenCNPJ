using System.Text.Json;
using System.Text.RegularExpressions;

namespace CNPJExporter.Modules.Receita.Processors;

/// <summary>
/// CNPJs com campos pessoais a anonimizar (lgpd.json na raiz do repositório): { "CNPJ": ["campo", ...] }.
/// </summary>
public sealed partial class LgpdPolicy
{
    public const string FileName = "lgpd.json";

    private static readonly HashSet<string> AllowedFields = new(StringComparer.Ordinal)
    {
        "razao_social",
        "nome_fantasia",
        "endereco",
        "email",
        "telefones"
    };

    private readonly IReadOnlyDictionary<string, string[]> _entries;

    private LgpdPolicy(IReadOnlyDictionary<string, string[]> entries)
    {
        _entries = entries;
    }

    public static LgpdPolicy Empty { get; } = new(new Dictionary<string, string[]>());

    public static LgpdPolicy LoadDefault() => Load(Path.Combine(AppContext.BaseDirectory, FileName));

    // Sem fallback: publicar sem a lista exporia os dados que deveriam estar ocultos.
    public static LgpdPolicy Load(string path)
    {
        var raw = JsonSerializer.Deserialize<Dictionary<string, string[]>>(File.ReadAllText(path))
                  ?? throw new InvalidOperationException($"{FileName} vazio ou inválido.");

        var entries = new Dictionary<string, string[]>(StringComparer.Ordinal);
        foreach (var (cnpj, fields) in raw)
        {
            var normalized = NonAlphanumeric().Replace(cnpj.ToUpperInvariant(), string.Empty);
            if (!CnpjPattern().IsMatch(normalized))
                throw new InvalidOperationException($"{FileName}: CNPJ inválido '{cnpj}'.");

            var unknown = fields.FirstOrDefault(field => !AllowedFields.Contains(field));
            if (unknown is not null)
                throw new InvalidOperationException(
                    $"{FileName}: campo '{unknown}' não suportado para {cnpj}. Use: {string.Join(", ", AllowedFields)}.");

            if (!entries.TryAdd(normalized, fields.Distinct(StringComparer.Ordinal).ToArray()))
                throw new InvalidOperationException($"{FileName}: CNPJ duplicado '{cnpj}'.");
        }

        return new LgpdPolicy(entries);
    }

    internal string BuildCte()
    {
        if (_entries.Count == 0)
            return "lgpd AS (SELECT CAST(NULL AS VARCHAR) AS cnpj, CAST(NULL AS VARCHAR[]) AS campos WHERE FALSE)";

        var rows = _entries.Select(entry =>
            $"('{Sql.EscapeLiteral(entry.Key)}', [{string.Join(", ", entry.Value.Select(field => $"'{Sql.EscapeLiteral(field)}'"))}]::VARCHAR[])");
        return $"lgpd AS (SELECT * FROM (VALUES {string.Join(", ", rows)}) AS t(cnpj, campos))";
    }

    [GeneratedRegex("[^A-Z0-9]")]
    private static partial Regex NonAlphanumeric();

    [GeneratedRegex("^[A-Z0-9]{12}[0-9]{2}$")]
    private static partial Regex CnpjPattern();
}
