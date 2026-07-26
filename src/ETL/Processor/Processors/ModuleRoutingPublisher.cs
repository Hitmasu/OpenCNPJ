using CNPJExporter.Exporters;
using CNPJExporter.Processors.Models;
using CNPJExporter.Utils;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class ModuleRoutingPublisher : IModuleRoutingPublisher
{
    public async Task<ModuleRoutingPublishResult> PublishAsync(
        string moduleKey,
        string releaseId,
        string? previousRoutingReleaseId,
        IReadOnlyDictionary<string, string> changedSegmentShardDirectories,
        IReadOnlyCollection<string> removedSegmentIds,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var routingRoot = Path.Combine(
            outputRootDir,
            "shards",
            "modules",
            moduleKey,
            "routing",
            "releases");
        var previousRoutingDirectory = string.IsNullOrWhiteSpace(
            previousRoutingReleaseId)
            ? null
            : Path.Combine(routingRoot, previousRoutingReleaseId);

        if (previousRoutingDirectory is not null
            && !HasRoutingFiles(previousRoutingDirectory))
        {
            var downloaded = await RcloneClient.DownloadFolderAsync(
                BuildRemoteRoutingDirectory(
                    moduleKey,
                    previousRoutingReleaseId!),
                previousRoutingDirectory);
            if (!downloaded || !HasRoutingFiles(previousRoutingDirectory))
            {
                throw new InvalidOperationException(
                    $"Não foi possível recuperar o roteamento anterior do módulo {moduleKey}.");
            }
        }

        var outputDirectory = Path.Combine(routingRoot, releaseId.Trim('/'));
        var result = BuildLocal(
            releaseId,
            previousRoutingDirectory,
            changedSegmentShardDirectories,
            removedSegmentIds,
            outputDirectory);
        if (result.GeneratedPrefixes.Count == 0)
            return result;

        var relativeFiles = result.GeneratedPrefixes
            .Select(prefix => $"{prefix}.routing.bin")
            .ToArray();
        AnsiConsole.MarkupLine(
            $"[cyan]Publicando roteamento do módulo {moduleKey.EscapeMarkup()}...[/] [grey](prefixos: {relativeFiles.Length})[/]");
        var uploaded = await RcloneClient.UploadSelectedFilesAsync(
            result.LocalRoutingDirectory,
            BuildRemoteRoutingDirectory(moduleKey, releaseId),
            relativeFiles);
        if (!uploaded)
        {
            throw new InvalidOperationException(
                $"Falha ao publicar o roteamento do módulo {moduleKey}.");
        }

        return result;
    }

    internal static ModuleRoutingPublishResult BuildLocal(
        string releaseId,
        string? previousRoutingDirectory,
        IReadOnlyDictionary<string, string> changedSegmentShardDirectories,
        IReadOnlyCollection<string> removedSegmentIds,
        string outputDirectory)
    {
        var builder = new SegmentRoutingIndexBuilder();
        if (!string.IsNullOrWhiteSpace(previousRoutingDirectory))
            builder.LoadDirectory(previousRoutingDirectory);

        foreach (var segmentId in removedSegmentIds
                     .Concat(changedSegmentShardDirectories.Keys)
                     .Distinct(StringComparer.Ordinal))
        {
            builder.RemoveSegment(segmentId);
        }

        foreach (var (segmentId, shardDirectory) in
                 changedSegmentShardDirectories.OrderBy(
                     item => item.Key,
                     StringComparer.Ordinal))
        {
            builder.AddSegment(segmentId, shardDirectory);
        }

        var prefixes = builder.WriteDirectory(outputDirectory);
        return new ModuleRoutingPublishResult(
            releaseId,
            outputDirectory,
            prefixes);
    }

    private static string BuildRemoteRoutingDirectory(
        string moduleKey,
        string releaseId) =>
        $"shards/modules/{moduleKey.Trim('/')}/routing/{releaseId.Trim('/')}";

    private static bool HasRoutingFiles(string directory) =>
        Directory.Exists(directory)
        && Directory.EnumerateFiles(
            directory,
            "*.routing.bin",
            SearchOption.TopDirectoryOnly).Any();
}
