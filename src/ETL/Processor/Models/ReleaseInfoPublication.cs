using CNPJExporter.Integrations;

namespace CNPJExporter.Processors.Models;

internal sealed record ReleaseInfoPublication(
    string? DatasetKey,
    string ReceitaDatasetKey,
    long Total,
    int ShardCount,
    string LastUpdated,
    int ShardPrefixLength,
    string StorageReleaseId,
    string PublicationReleaseId,
    string DefaultShardReleaseId,
    IReadOnlyDictionary<string, string> ShardReleases,
    IReadOnlyList<DataIntegrationRunSummary> IntegrationSummaries,
    IReadOnlyDictionary<string, ModuleShardPublication> ModuleShards);
