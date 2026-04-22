using CNPJExporter.Processors.Models;

namespace CNPJExporter.Processors;

internal interface IShardZipPublisher
{
    Task<ZipArtifactPublication> PublishBaseAsync(
        string datasetKey,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default);

    Task<ZipArtifactPublication> PublishModuleAsync(
        string moduleKey,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default);
}
