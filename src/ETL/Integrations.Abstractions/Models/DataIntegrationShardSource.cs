namespace CNPJExporter.Integrations;

public sealed record DataIntegrationShardSource(
    string Key,
    string JsonPropertyName,
    string SchemaVersion,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    string? ParquetGlob,
    long RecordCount,
    string? SegmentCollectionProperty = null,
    IReadOnlyList<DataIntegrationSegment>? Segments = null)
{
    public IReadOnlyList<DataIntegrationSegment> EffectiveSegments =>
        Segments ?? [];

    public static IReadOnlyList<DataIntegrationShardSource> FromRunSummaries(
        IEnumerable<DataIntegrationRunSummary> summaries) =>
        summaries
            .Where(summary =>
                !string.IsNullOrWhiteSpace(summary.ParquetGlob)
                || summary.Segments is { Count: > 0 })
            .Select(summary => new DataIntegrationShardSource(
                summary.Descriptor.Key,
                summary.Descriptor.JsonPropertyName,
                summary.Descriptor.SchemaVersion,
                summary.SourceVersion,
                summary.UpdatedAt,
                summary.ParquetGlob,
                summary.RecordCount,
                summary.Descriptor.SegmentCollectionProperty,
                summary.Segments))
            .ToArray();
}
