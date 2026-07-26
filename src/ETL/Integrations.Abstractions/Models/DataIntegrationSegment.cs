namespace CNPJExporter.Integrations;

public sealed record DataIntegrationSegment(
    string Id,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    string ParquetGlob,
    long RecordCount,
    IReadOnlyList<string>? ReplacesSegmentIds = null)
{
    public IReadOnlyList<string> EffectiveReplacesSegmentIds =>
        ReplacesSegmentIds ?? [];
}
