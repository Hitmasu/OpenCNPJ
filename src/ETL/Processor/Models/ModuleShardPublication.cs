namespace CNPJExporter.Processors.Models;

internal sealed record ModuleShardPublication(
    string Key,
    string JsonPropertyName,
    string SchemaVersion,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    long RecordCount,
    string StorageReleaseId,
    ZipArtifactPublication Zip,
    string? RoutingReleaseId = null,
    string? SegmentCollectionProperty = null,
    IReadOnlyList<ModuleSegmentPublication>? Segments = null)
{
    public IReadOnlyList<ModuleSegmentPublication> EffectiveSegments =>
        Segments ?? [];

    public bool IsSegmented =>
        !string.IsNullOrWhiteSpace(RoutingReleaseId)
        && EffectiveSegments.Count > 0;

    public static ModuleShardPublication FromPublished(PublishedModuleShardSnapshot snapshot) =>
        new(
            snapshot.Key,
            snapshot.JsonPropertyName,
            snapshot.SchemaVersion,
            snapshot.SourceVersion,
            snapshot.UpdatedAt,
            snapshot.RecordCount,
            snapshot.StorageReleaseId,
            snapshot.Zip.ToPublication(),
            snapshot.RoutingReleaseId,
            snapshot.SegmentCollectionProperty,
            snapshot.EffectiveSegments
                .Select(segment => segment.ToPublication())
                .ToArray());
}
