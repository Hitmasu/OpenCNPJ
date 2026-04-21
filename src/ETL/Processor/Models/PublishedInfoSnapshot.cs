namespace CNPJExporter.Processors.Models;

internal sealed record PublishedInfoSnapshot(
    long? Total,
    int? ShardCount,
    string? LastUpdated,
    string? StorageReleaseId,
    IReadOnlyDictionary<string, PublishedModuleShardSnapshot> ModuleShards);
