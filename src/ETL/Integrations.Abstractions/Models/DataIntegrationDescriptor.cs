namespace CNPJExporter.Integrations;

public sealed record DataIntegrationDescriptor(
    string Key,
    string JsonPropertyName,
    TimeSpan RefreshInterval,
    string SchemaVersion)
{
    private static readonly System.Text.RegularExpressions.Regex IdentifierPattern = new(
        "^[A-Za-z_][A-Za-z0-9_]*$",
        System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Key))
            throw new ArgumentException("A chave da integração é obrigatória.", nameof(Key));

        if (!IdentifierPattern.IsMatch(Key))
            throw new ArgumentException("A chave da integração deve ser um identificador ASCII simples.", nameof(Key));

        if (string.IsNullOrWhiteSpace(JsonPropertyName))
            throw new ArgumentException("O nome da propriedade JSON da integração é obrigatório.", nameof(JsonPropertyName));

        if (!IdentifierPattern.IsMatch(JsonPropertyName))
            throw new ArgumentException("A propriedade JSON da integração deve ser um identificador ASCII simples.", nameof(JsonPropertyName));

        if (RefreshInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(RefreshInterval), "A frequência da integração deve ser positiva.");

        if (string.IsNullOrWhiteSpace(SchemaVersion))
            throw new ArgumentException("A versão de schema da integração é obrigatória.", nameof(SchemaVersion));
    }
}
