namespace CNPJExporter.Integrations;

public sealed record DataIntegrationHashState(
    Dictionary<string, string> Hashes,
    string? SourceVersion = null,
    DateTimeOffset? UpdatedAt = null,
    string? ParquetGlob = null,
    string? SchemaVersion = null)
{
    public static DataIntegrationHashState Empty { get; } = new(new Dictionary<string, string>(StringComparer.Ordinal));
}
