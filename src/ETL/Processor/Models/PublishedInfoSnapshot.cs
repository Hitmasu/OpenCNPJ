namespace CNPJExporter.Processors.Models;

internal sealed record PublishedInfoSnapshot(
    long? Total,
    int? ShardCount,
    string? LastUpdated,
    string? StorageReleaseId,
    string? DefaultShardReleaseId,
    IReadOnlyDictionary<string, string> ShardReleases,
    IReadOnlyDictionary<string, PublishedModuleShardSnapshot> ModuleShards);
