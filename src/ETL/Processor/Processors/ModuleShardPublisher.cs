using CNPJExporter.Configuration;
using CNPJExporter.Integrations;
using CNPJExporter.Processors.Models;
using CNPJExporter.Utils;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class ModuleShardPublisher
{
    private readonly ModuleShardExporter _exporter;

    public ModuleShardPublisher(ModuleShardExporter? exporter = null)
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
            var publishAll = previousPublication is null || schemaChanged || summary.RequiresFullPublish;
            var shouldPublish = publishAll || summary.HasPublicationChanges;

            if (!shouldPublish)
                continue;

            var prefixesToRegenerate = publishAll ? null : GetChangedPrefixes(summary);
            AnsiConsole.MarkupLine(
                publishAll
                    ? $"[cyan]Publicando todos os shards do módulo {source.Key.EscapeMarkup()}...[/]"
                    : $"[cyan]Publicando {prefixesToRegenerate!.Count} prefixo(s) alterado(s) do módulo {source.Key.EscapeMarkup()}...[/]");

            await _exporter.ExportAndUploadAsync(
                source,
                releaseId,
                outputRootDir,
                prefixesToRegenerate,
                cancellationToken);

            publications[source.Key] = BuildPublication(
                source,
                summary,
                previousPublication,
                releaseId,
                publishAll,
                prefixesToRegenerate);
        }

        return publications;
    }

    private static ModuleShardPublication BuildPublication(
        DataIntegrationShardSource source,
        DataIntegrationRunSummary summary,
        ModuleShardPublication? previousPublication,
        string releaseId,
        bool publishAll,
        IReadOnlyCollection<string>? prefixesToRegenerate)
    {
        if (publishAll || previousPublication is null)
        {
            return new ModuleShardPublication(
                source.Key,
                source.JsonPropertyName,
                source.SchemaVersion,
                source.SourceVersion,
                source.UpdatedAt,
                source.RecordCount,
                releaseId,
                releaseId,
                new Dictionary<string, string>(StringComparer.Ordinal));
        }

        var shardReleases = new Dictionary<string, string>(
            previousPublication.ShardReleases,
            StringComparer.Ordinal);

        foreach (var prefix in prefixesToRegenerate ?? [])
            shardReleases[prefix] = releaseId;

        return new ModuleShardPublication(
            source.Key,
            source.JsonPropertyName,
            source.SchemaVersion,
            source.SourceVersion,
            summary.UpdatedAt,
            summary.RecordCount,
            releaseId,
            previousPublication.DefaultShardReleaseId,
            shardReleases);
    }

    private static IReadOnlyCollection<string> GetChangedPrefixes(DataIntegrationRunSummary summary)
    {
        return summary.ChangedCnpjs
            .Where(cnpj => cnpj.Length >= AppConfig.Current.Shards.PrefixLength)
            .Select(cnpj => cnpj[..AppConfig.Current.Shards.PrefixLength])
            .Distinct(StringComparer.Ordinal)
            .OrderBy(prefix => prefix, StringComparer.Ordinal)
            .ToArray();
    }
}
