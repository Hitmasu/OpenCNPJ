using CNPJExporter.Processors.Models;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class NoopShardZipPublisher : IShardZipPublisher
{
    public Task<ZipArtifactPublication> PublishBaseAsync(
        string datasetKey,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        AnsiConsole.MarkupLine($"[yellow]ZIP do dataset {datasetKey.EscapeMarkup()} ignorado por --skip-zip.[/]");
        return Task.FromResult(ZipArtifactPublication.Missing);
    }

    public Task<ZipArtifactPublication> PublishModuleAsync(
        string moduleKey,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        AnsiConsole.MarkupLine($"[yellow]ZIP do módulo {moduleKey.EscapeMarkup()} ignorado por --skip-zip.[/]");
        return Task.FromResult(ZipArtifactPublication.Missing);
    }
}
