using CNPJExporter.Integrations;
using CNPJExporter.Processors.Models;

namespace CNPJExporter.Processors;

internal interface IModuleShardExporter
{
    Task<ModuleShardExportResult> ExportAndUploadAsync(
        DataIntegrationShardSource source,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default);

    Task<ModuleShardExportResult> ExportSegmentAndUploadAsync(
        DataIntegrationShardSource source,
        DataIntegrationSegment segment,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(
            "O exportador não implementa segmentos históricos.");
}
