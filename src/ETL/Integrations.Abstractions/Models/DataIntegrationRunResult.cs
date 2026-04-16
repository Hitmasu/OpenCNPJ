namespace CNPJExporter.Integrations;

public sealed record DataIntegrationRunResult(
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    string? ParquetGlob,
    long RecordCount,
    IReadOnlyDictionary<string, string> CurrentHashes)
{
    public static DataIntegrationRunResult Unchanged(DataIntegrationHashState previousState, DateTimeOffset now) =>
        new(
            previousState.SourceVersion,
            previousState.UpdatedAt ?? now,
            previousState.ParquetGlob,
            previousState.Hashes.Count,
            previousState.Hashes);
}
