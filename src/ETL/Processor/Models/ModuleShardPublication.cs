namespace CNPJExporter.Processors.Models;

internal sealed record ModuleShardPublication(
    string Key,
    string JsonPropertyName,
    string SchemaVersion,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    long RecordCount,
    string StorageReleaseId,
    string DefaultShardReleaseId,
    IReadOnlyDictionary<string, string> ShardReleases)
{
    public static ModuleShardPublication FromPublished(PublishedModuleShardSnapshot snapshot) =>
        new(
            snapshot.Key,
            snapshot.JsonPropertyName,
            snapshot.SchemaVersion,
            snapshot.SourceVersion,
            snapshot.UpdatedAt,
            snapshot.RecordCount,
            snapshot.StorageReleaseId,
            snapshot.DefaultShardReleaseId,
            snapshot.ShardReleases);
}
