using CNPJExporter.Integrations;
using CNPJExporter.Exporters;
using CNPJExporter.Processors.Models;
using CNPJExporter.Utils;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class ModuleShardPublisher
{
    private readonly IModuleShardExporter _exporter;
    private readonly IShardZipPublisher _zipPublisher;
    private readonly IModuleRoutingPublisher _routingPublisher;

    public ModuleShardPublisher(
        IModuleShardExporter? exporter = null,
        IShardZipPublisher? zipPublisher = null,
        IModuleRoutingPublisher? routingPublisher = null)
    {
        _exporter = exporter ?? new ModuleShardExporter();
        _zipPublisher = zipPublisher ?? new ShardZipPublisher();
        _routingPublisher = routingPublisher ?? new ModuleRoutingPublisher();
    }

    public async Task<IReadOnlyDictionary<string, ModuleShardPublication>> PublishAsync(
        string releaseId,
        IReadOnlyList<DataIntegrationRunSummary> integrationSummaries,
        PublishedInfoSnapshot? publishedInfo,
        string outputRootDir,
        bool resumeExistingRelease = false,
        CancellationToken cancellationToken = default)
    {
        var publications = publishedInfo is null
            ? new Dictionary<string, ModuleShardPublication>(StringComparer.Ordinal)
            : publishedInfo.ModuleShards.ToDictionary(
                kvp => kvp.Key,
                kvp => ModuleShardPublication.FromPublished(kvp.Value),
                StringComparer.Ordinal);

        var summariesByKey = integrationSummaries.ToDictionary(summary => summary.Descriptor.Key, StringComparer.Ordinal);

        foreach (var source in DataIntegrationShardSource.FromRunSummaries(integrationSummaries))
        {
            var summary = summariesByKey[source.Key];
            publications.TryGetValue(source.Key, out var previousPublication);

            var schemaChanged = previousPublication is not null
                                && !string.Equals(
                                    previousPublication.SchemaVersion,
                                    source.SchemaVersion,
                                    StringComparison.Ordinal);
            var shouldPublish = previousPublication is null
                                || schemaChanged
                                || summary.RequiresFullPublish
                                || summary.HasPublicationChanges;

            if (!shouldPublish)
                continue;

            var publicationReleaseId = releaseId;
            if (resumeExistingRelease
                && source.EffectiveSegments.Count == 0
                && await TryResumeModuleAsync(
                    source,
                    summary,
                    publicationReleaseId,
                    outputRootDir,
                    cancellationToken) is { } resumedModule)
            {
                AnsiConsole.MarkupLine(
                    $"[green]✓ Módulo {source.Key.EscapeMarkup()} reutilizado[/] [grey](release: {publicationReleaseId.EscapeMarkup()})[/]");
                publications[source.Key] = resumedModule;
                continue;
            }

            AnsiConsole.MarkupLine(
                $"[grey]Módulo {source.Key.EscapeMarkup()}:[/] executando publicação [cyan]shards+zip[/] [grey](release efetivo: {publicationReleaseId.EscapeMarkup()})[/]");
            if (source.EffectiveSegments.Count > 0)
            {
                publications[source.Key] = await PublishSegmentedModuleAsync(
                    source,
                    summary,
                    previousPublication,
                    schemaChanged,
                    publicationReleaseId,
                    outputRootDir,
                    resumeExistingRelease,
                    cancellationToken);
                continue;
            }

            var publication = await PublishChangedModuleAsync(source, summary, publicationReleaseId, outputRootDir, cancellationToken);

            AnsiConsole.MarkupLine(
                $"[cyan]Gerando ZIP do módulo {source.Key.EscapeMarkup()}...[/] [grey](release efetivo: {publicationReleaseId.EscapeMarkup()})[/]");
            var zip = await _zipPublisher.PublishModuleAsync(
                source.Key,
                publicationReleaseId,
                outputRootDir,
                cancellationToken);
            publication = publication with { Zip = zip };

            publications[source.Key] = publication;
        }

        return publications;
    }

    private async Task<ModuleShardPublication> PublishSegmentedModuleAsync(
        DataIntegrationShardSource source,
        DataIntegrationRunSummary summary,
        ModuleShardPublication? previousPublication,
        bool schemaChanged,
        string releaseId,
        string outputRootDir,
        bool resumeExistingRelease,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(source.SegmentCollectionProperty))
        {
            throw new InvalidOperationException(
                $"O módulo segmentado {source.Key} deve informar SegmentCollectionProperty.");
        }

        var previousSegments = previousPublication?.IsSegmented == true
            ? previousPublication.EffectiveSegments.ToDictionary(
                segment => segment.Id,
                StringComparer.Ordinal)
            : new Dictionary<string, ModuleSegmentPublication>(
                StringComparer.Ordinal);
        var sourceSegmentIds = source.EffectiveSegments
            .Select(segment => segment.Id)
            .ToHashSet(StringComparer.Ordinal);

        if (schemaChanged)
        {
            var missingSegments = previousSegments.Keys
                .Where(id => !sourceSegmentIds.Contains(id))
                .ToArray();
            if (missingSegments.Length > 0)
            {
                throw new InvalidOperationException(
                    $"Mudança de schema do módulo {source.Key} exige regenerar todos os segmentos; ausentes: {string.Join(", ", missingSegments)}.");
            }

            previousSegments.Clear();
        }

        var removedSegmentIds = source.EffectiveSegments
            .SelectMany(segment => segment.EffectiveReplacesSegmentIds)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        foreach (var removedSegmentId in removedSegmentIds)
            previousSegments.Remove(removedSegmentId);

        var changedShardDirectories = new Dictionary<string, string>(
            StringComparer.Ordinal);
        foreach (var segment in source.EffectiveSegments
                     .OrderBy(segment => segment.Id, StringComparer.Ordinal))
        {
            var unchanged = previousSegments.TryGetValue(
                                segment.Id,
                                out var previousSegment)
                            && string.Equals(
                                previousSegment.SourceVersion,
                                segment.SourceVersion,
                                StringComparison.Ordinal);
            if (unchanged)
                continue;

            if (resumeExistingRelease
                && await TryResumeSegmentAsync(
                    source,
                    segment,
                    releaseId,
                    outputRootDir,
                    cancellationToken) is { } resumedSegment)
            {
                AnsiConsole.MarkupLine(
                    $"[green]✓ Segmento {source.Key.EscapeMarkup()}/{segment.Id.EscapeMarkup()} reutilizado[/] [grey](release: {releaseId.EscapeMarkup()})[/]");
                previousSegments[segment.Id] = resumedSegment;
                continue;
            }

            AnsiConsole.MarkupLine(
                $"[cyan]Publicando segmento {source.Key.EscapeMarkup()}/{segment.Id.EscapeMarkup()}...[/] [grey](registros: {segment.RecordCount:N0})[/]");
            var export = await _exporter.ExportSegmentAndUploadAsync(
                source,
                segment,
                releaseId,
                outputRootDir,
                cancellationToken);
            var zip = await _zipPublisher.PublishModuleSegmentAsync(
                source.Key,
                segment.Id,
                releaseId,
                outputRootDir,
                cancellationToken);
            previousSegments[segment.Id] = new ModuleSegmentPublication(
                segment.Id,
                segment.SourceVersion,
                segment.UpdatedAt,
                segment.RecordCount,
                releaseId,
                zip);
            changedShardDirectories[segment.Id] = export.LocalShardDir;
        }

        var previousRoutingReleaseId = previousPublication?.RoutingReleaseId;
        var routingChanged = changedShardDirectories.Count > 0
                             || removedSegmentIds.Length > 0
                             || string.IsNullOrWhiteSpace(
                                 previousRoutingReleaseId);
        var routingReleaseId = previousRoutingReleaseId;
        if (resumeExistingRelease
            && changedShardDirectories.Count == 0
            && previousSegments.Values.Any(segment =>
                string.Equals(
                    segment.StorageReleaseId,
                    releaseId,
                    StringComparison.Ordinal))
            && await CanResumeRoutingAsync(
                source.Key,
                releaseId,
                outputRootDir,
                cancellationToken))
        {
            routingReleaseId = releaseId;
            routingChanged = false;
            AnsiConsole.MarkupLine(
                $"[green]✓ Roteamento do módulo {source.Key.EscapeMarkup()} reutilizado[/] [grey](release: {releaseId.EscapeMarkup()})[/]");
        }

        if (routingChanged)
        {
            var routing = await _routingPublisher.PublishAsync(
                source.Key,
                releaseId,
                previousRoutingReleaseId,
                changedShardDirectories,
                removedSegmentIds,
                outputRootDir,
                cancellationToken);
            routingReleaseId = routing.RoutingReleaseId;
        }

        if (string.IsNullOrWhiteSpace(routingReleaseId))
        {
            throw new InvalidOperationException(
                $"O módulo segmentado {source.Key} não produziu roteamento.");
        }

        var activeSegments = previousSegments.Values
            .OrderBy(segment => segment.Id, StringComparer.Ordinal)
            .ToArray();
        return new ModuleShardPublication(
            source.Key,
            source.JsonPropertyName,
            source.SchemaVersion,
            source.SourceVersion,
            summary.UpdatedAt,
            activeSegments.Sum(segment => segment.RecordCount),
            routingReleaseId,
            ZipArtifactPublication.Missing,
            routingReleaseId,
            source.SegmentCollectionProperty,
            activeSegments);
    }

    private async Task<ModuleShardPublication> PublishChangedModuleAsync(
        DataIntegrationShardSource source,
        DataIntegrationRunSummary summary,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken)
    {
        AnsiConsole.MarkupLine(
            $"[cyan]Publicando todos os shards do módulo {source.Key.EscapeMarkup()}...[/] [grey](schema: {source.SchemaVersion.EscapeMarkup()}, source_version: {(source.SourceVersion ?? "n/a").EscapeMarkup()}, registros: {summary.RecordCount:N0})[/]");

        await _exporter.ExportAndUploadAsync(
            source,
            releaseId,
            outputRootDir,
            cancellationToken);

        return BuildPublication(
            source,
            summary,
            releaseId);
    }

    private static ModuleShardPublication BuildPublication(
        DataIntegrationShardSource source,
        DataIntegrationRunSummary summary,
        string releaseId)
    {
        return new ModuleShardPublication(
            source.Key,
            source.JsonPropertyName,
            source.SchemaVersion,
            source.SourceVersion,
            summary.UpdatedAt,
            summary.RecordCount,
            releaseId,
            ZipArtifactPublication.Missing);
    }

    private static async Task<ModuleShardPublication?> TryResumeModuleAsync(
        DataIntegrationShardSource source,
        DataIntegrationRunSummary summary,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken)
    {
        var localDirectory = Path.Combine(
            outputRootDir,
            "shards",
            "modules",
            source.Key,
            "releases",
            releaseId.Trim('/'));
        var zip = await TryResumeArtifactsAsync(
            localDirectory,
            $"shards/modules/{source.Key}/{releaseId.Trim('/')}",
            $"releases/{source.Key}",
            $"https://file.opencnpj.org/releases/{source.Key}/data.zip",
            cancellationToken);
        return zip is null
            ? null
            : new ModuleShardPublication(
                source.Key,
                source.JsonPropertyName,
                source.SchemaVersion,
                source.SourceVersion,
                summary.UpdatedAt,
                summary.RecordCount,
                releaseId,
                zip);
    }

    private static async Task<ModuleSegmentPublication?> TryResumeSegmentAsync(
        DataIntegrationShardSource source,
        DataIntegrationSegment segment,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken)
    {
        var localDirectory = Path.Combine(
            outputRootDir,
            "shards",
            "modules",
            source.Key,
            "segments",
            segment.Id,
            "releases",
            releaseId.Trim('/'));
        var zip = await TryResumeArtifactsAsync(
            localDirectory,
            $"shards/modules/{source.Key}/segments/{segment.Id}/{releaseId.Trim('/')}",
            $"releases/{source.Key}/segments/{segment.Id}",
            $"https://file.opencnpj.org/releases/{source.Key}/segments/{segment.Id}/data.zip",
            cancellationToken);
        return zip is null
            ? null
            : new ModuleSegmentPublication(
                segment.Id,
                segment.SourceVersion,
                segment.UpdatedAt,
                segment.RecordCount,
                releaseId,
                zip);
    }

    private static async Task<ZipArtifactPublication?> TryResumeArtifactsAsync(
        string localDirectory,
        string remoteShardDirectory,
        string remoteZipDirectory,
        string publicZipUrl,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryGetCompletedArtifactFiles(
                localDirectory,
                out var shardFiles,
                out var zipPath))
        {
            return null;
        }

        var remoteShards = await RcloneClient.ListRemoteChecksumsAsync(
            remoteShardDirectory,
            "*.ndjson",
            "*.index.bin");
        if (shardFiles.Any(path =>
                !remoteShards.ContainsKey(Path.GetFileName(path))))
        {
            return null;
        }

        var remoteZip = await RcloneClient.ListRemoteChecksumsAsync(
            remoteZipDirectory,
            "data.zip");
        if (!remoteZip.TryGetValue("data.zip", out var zipChecksum))
            return null;

        return new ZipArtifactPublication(
            true,
            new FileInfo(zipPath).Length,
            publicZipUrl,
            zipChecksum);
    }

    internal static bool TryGetCompletedArtifactFiles(
        string localDirectory,
        out IReadOnlyList<string> shardFiles,
        out string zipPath)
    {
        shardFiles = [];
        zipPath = Path.Combine(localDirectory, "data.zip");
        if (!Directory.Exists(localDirectory)
            || !File.Exists(zipPath)
            || Directory.EnumerateFiles(
                    localDirectory,
                    "*.tmp",
                    SearchOption.TopDirectoryOnly).Any())
        {
            return false;
        }

        var ndjson = Directory.EnumerateFiles(
                localDirectory,
                "*.ndjson",
                SearchOption.TopDirectoryOnly)
            .Order(StringComparer.Ordinal)
            .ToArray();
        var indexes = Directory.EnumerateFiles(
                localDirectory,
                "*.index.bin",
                SearchOption.TopDirectoryOnly)
            .Order(StringComparer.Ordinal)
            .ToArray();
        if (ndjson.Length == 0
            || ndjson.Length != indexes.Length
            || ndjson.Any(path =>
                !File.Exists(
                    Path.Combine(
                        localDirectory,
                        $"{Path.GetFileNameWithoutExtension(path)}.index.bin"))))
        {
            return false;
        }

        shardFiles = ndjson.Concat(indexes).ToArray();
        return true;
    }

    private static async Task<bool> CanResumeRoutingAsync(
        string moduleKey,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var localDirectory = Path.Combine(
            outputRootDir,
            "shards",
            "modules",
            moduleKey,
            "routing",
            "releases",
            releaseId.Trim('/'));
        if (!Directory.Exists(localDirectory))
            return false;

        var localFiles = Directory.EnumerateFiles(
                localDirectory,
                "*.routing.bin",
                SearchOption.TopDirectoryOnly)
            .Select(Path.GetFileName)
            .OfType<string>()
            .ToArray();
        if (localFiles.Length == 0)
            return false;

        var remoteFiles = await RcloneClient.ListRemoteChecksumsAsync(
            $"shards/modules/{moduleKey}/routing/{releaseId.Trim('/')}",
            "*.routing.bin");
        return localFiles.All(remoteFiles.ContainsKey);
    }
}
