using CNPJExporter.Integrations;
using CNPJExporter.Processors.Models;
using CNPJExporter.Utils;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class ModuleShardPublisher
{
    private readonly IModuleShardExporter _exporter;
    private readonly IShardZipPublisher _zipPublisher;

    public ModuleShardPublisher(
        IModuleShardExporter? exporter = null,
        IShardZipPublisher? zipPublisher = null)
    {
        _exporter = exporter ?? new ModuleShardExporter();
        _zipPublisher = zipPublisher ?? new ShardZipPublisher();
    }

    public async Task<IReadOnlyDictionary<string, ModuleShardPublication>> PublishAsync(
        string releaseId,
        IReadOnlyList<DataIntegrationRunSummary> integrationSummaries,
        PublishedInfoSnapshot? publishedInfo,
        string outputRootDir,
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
            AnsiConsole.MarkupLine(
                $"[grey]Módulo {source.Key.EscapeMarkup()}:[/] executando publicação [cyan]shards+zip[/] [grey](release efetivo: {publicationReleaseId.EscapeMarkup()})[/]");
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
}
