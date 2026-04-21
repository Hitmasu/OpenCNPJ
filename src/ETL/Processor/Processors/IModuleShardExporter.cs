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
}
