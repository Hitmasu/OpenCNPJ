namespace CNPJExporter.Processors.Models;

internal sealed record PublishedModuleShardSnapshot(
    string Key,
    string JsonPropertyName,
    string SchemaVersion,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    long RecordCount,
    string StorageReleaseId,
    string DefaultShardReleaseId,
    IReadOnlyDictionary<string, string> ShardReleases);
