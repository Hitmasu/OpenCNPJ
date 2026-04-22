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
    ZipArtifactPublication BaseZip,
    IReadOnlyList<DataIntegrationRunSummary> IntegrationSummaries,
    IReadOnlyDictionary<string, ModuleShardPublication> ModuleShards);
