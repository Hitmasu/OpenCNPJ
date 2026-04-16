namespace CNPJExporter.Integrations;

public sealed record DataIntegrationShardSource(
    string Key,
    string JsonPropertyName,
    string SchemaVersion,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    string ParquetGlob,
    long RecordCount)
{
    public static IReadOnlyList<DataIntegrationShardSource> FromRunSummaries(
        IEnumerable<DataIntegrationRunSummary> summaries) =>
        summaries
            .Where(summary => !string.IsNullOrWhiteSpace(summary.ParquetGlob))
            .Select(summary => new DataIntegrationShardSource(
                summary.Descriptor.Key,
                summary.Descriptor.JsonPropertyName,
                summary.Descriptor.SchemaVersion,
                summary.SourceVersion,
                summary.UpdatedAt,
                summary.ParquetGlob!,
                summary.RecordCount))
            .ToArray();
}
