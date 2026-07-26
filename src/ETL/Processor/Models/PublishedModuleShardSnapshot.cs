namespace CNPJExporter.Processors.Models;

internal sealed record PublishedModuleShardSnapshot(
    string Key,
    string JsonPropertyName,
    string SchemaVersion,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    long RecordCount,
    string StorageReleaseId,
    PublishedZipArtifactSnapshot Zip,
    string? RoutingReleaseId = null,
    string? SegmentCollectionProperty = null,
    IReadOnlyList<PublishedModuleSegmentSnapshot>? Segments = null)
{
    public IReadOnlyList<PublishedModuleSegmentSnapshot> EffectiveSegments =>
        Segments ?? [];
}
