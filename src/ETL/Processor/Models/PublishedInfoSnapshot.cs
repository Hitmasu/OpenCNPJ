namespace CNPJExporter.Processors.Models;

internal sealed record PublishedInfoSnapshot(
    long? Total,
    int? ShardCount,
    string? LastUpdated,
    string? StorageReleaseId,
    PublishedZipArtifactSnapshot BaseZip,
    IReadOnlyDictionary<string, PublishedModuleShardSnapshot> ModuleShards);
