using CNPJExporter.Integrations;
using CNPJExporter.Processors.Models;
using CNPJExporter.Utils;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class ModuleShardPublisher
{
    private readonly IModuleShardExporter _exporter;

    public ModuleShardPublisher(IModuleShardExporter? exporter = null)
    {
        _exporter = exporter ?? new ModuleShardExporter();
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

            AnsiConsole.MarkupLine($"[cyan]Publicando todos os shards do módulo {source.Key.EscapeMarkup()}...[/]");

            await _exporter.ExportAndUploadAsync(
                source,
                releaseId,
                outputRootDir,
                cancellationToken);

            publications[source.Key] = BuildPublication(
                source,
                summary,
                releaseId);
        }

        return publications;
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
            releaseId);
    }
}
