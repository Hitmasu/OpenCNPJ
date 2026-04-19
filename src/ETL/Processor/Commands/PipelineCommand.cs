using System.ComponentModel;
using CNPJExporter.Configuration;
using CNPJExporter.Integrations;
using CNPJExporter.Modules.Receita;
using CNPJExporter.Modules.Receita.Downloaders;
using CNPJExporter.Processors.Models;
using CNPJExporter.Processors;
using CNPJExporter.Utils;
using System.Security.Cryptography;
using System.Text;
using Spectre.Console;
using Spectre.Console.Cli;

namespace CNPJExporter.Commands;

public class PipelineSettings : CommandSettings
{
    [CommandOption("--month|-m")]
    [Description("Mês (YYYY-MM). Se omitido, processa apenas o mês atual quando ele já estiver disponível na Receita.")]
    public string? Month { get; init; }

    [CommandOption("--cleanup-on-success")]
    [Description("Remove artefatos locais do dataset após processamento concluído com sucesso")]
    public bool CleanupOnSuccess { get; init; }

    [CommandOption("--release-id")]
    [Description("Identificador do release no storage. Se omitido, o ETL gera um hash curto automaticamente.")]
    public string? ReleaseId { get; init; }

}

public sealed class PipelineCommand : AsyncCommand<PipelineSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, PipelineSettings settings)
    {
        var totalSteps = settings.CleanupOnSuccess ? 6 : 5;
        var currentMonth = DatasetPublicationPolicy.GetCurrentMonth();
        var receitaIntegration = new DataIntegration();
        receitaIntegration.Descriptor.Validate();
        var integrations = DataIntegrationRegistry.CreateDefault();
        string? publishedMonth = null;
        string? publishedLastUpdated = null;
        PublishedInfoSnapshot? publishedInfo = null;
        var baseDatasetAlreadyCurrent = false;

        if (string.IsNullOrWhiteSpace(settings.Month))
        {
            var publishedInfoClient = new PublishedInfoClient();
            publishedInfo = await publishedInfoClient.GetPublishedInfoAsync();
            publishedLastUpdated = publishedInfo.LastUpdated;
            baseDatasetAlreadyCurrent =
                DatasetPublicationPolicy.TryGetPublishedMonth(publishedLastUpdated, out publishedMonth)
                && string.Equals(publishedMonth, currentMonth, StringComparison.Ordinal);

            if (baseDatasetAlreadyCurrent && integrations.Count == 0)
                return NoWork(currentMonth);
        }

        string selectedMonth;
        var receitaChanged = !baseDatasetAlreadyCurrent || !string.IsNullOrWhiteSpace(settings.Month);

        if (receitaChanged)
        {
            AnsiConsole.MarkupLine($"[cyan]1/{totalSteps} Baixando dados da Receita...[/]");
            var downloader = new Downloader(
                AppConfig.Current.Paths.DownloadDir,
                AppConfig.Current.Paths.DataDir,
                AppConfig.Current.Downloader.PublicShareRoot,
                AppConfig.Current.Downloader.ParallelDownloads);
            var availableMonths = await downloader.GetAvailableMonthsAsync();

            if (!DatasetPublicationPolicy.TrySelectMonthToProcess(settings.Month, availableMonths, out var monthToProcess, out var latestAvailableMonth))
            {
                if (integrations.Count == 0)
                {
                    AnsiConsole.MarkupLine(
                        $"[yellow]ℹ️ Nenhuma base nova para publicar. Último mês disponível na Receita: {latestAvailableMonth}. Mês atual esperado: {currentMonth}[/]");
                    return DatasetPublicationPolicy.NoNewDatasetExitCode;
                }

                monthToProcess = ResolveLocalBaseDatasetKey(publishedMonth, currentMonth);
                receitaChanged = false;
            }

            if (string.IsNullOrWhiteSpace(monthToProcess))
                throw new InvalidOperationException("Não foi possível resolver o mês da Receita para processamento.");

            selectedMonth = receitaChanged
                ? await downloader.DownloadAndExtractAsync(monthToProcess, availableMonths)
                : monthToProcess;
        }
        else
        {
            selectedMonth = ResolveLocalBaseDatasetKey(publishedMonth, currentMonth);
        }

        var releaseId = ResolveReleaseId(settings.ReleaseId, selectedMonth);

        AnsiConsole.MarkupLine($"[grey]Release id:[/] [cyan]{releaseId.EscapeMarkup()}[/]");

        AnsiConsole.MarkupLine($"[cyan]2/{totalSteps} Executando integrações de dados...[/]");
        var integrationStateRoot = Path.Combine(AppConfig.Current.Paths.DataDir, "integrations", "_state");
        var integrationOrchestrator = new DataIntegrationOrchestrator(
            integrations,
            new RcloneDataIntegrationStateStore(integrationStateRoot),
            BuildPublishedIntegrationStates(publishedInfo));
        var integrationSummaries = await integrationOrchestrator.RunAsync(selectedMonth, BuildIntegrationPaths());
        var hasIntegrationPublicationChanges = integrationSummaries.Any(summary => summary.HasPublicationChanges);
        var requiresModuleShardBootstrap = RequiresModuleShardBootstrap(integrationSummaries, publishedInfo);

        if (!receitaChanged && !hasIntegrationPublicationChanges && !requiresModuleShardBootstrap)
            return NoWork(currentMonth);

        if (!receitaChanged
            && string.IsNullOrWhiteSpace(publishedInfo?.StorageReleaseId))
        {
            throw new InvalidOperationException("Publicação de módulos exige um release base anterior publicado no info.json.");
        }

        using (var ingestor = new ParquetIngestor(selectedMonth))
        {
            if (receitaChanged)
            {
                AnsiConsole.MarkupLine($"[cyan]3/{totalSteps} Convertendo CSVs de {selectedMonth} para Parquet...[/]");
                await ingestor.ConvertCsvsToParquet();
            }
            else
            {
                AnsiConsole.MarkupLine($"[cyan]3/{totalSteps} Base da Receita sem alteração; shards base serão reutilizados do release publicado.[/]");
            }

            if (receitaChanged)
            {
                AnsiConsole.MarkupLine($"[cyan]4/{totalSteps} Gerando shards base e enviando {selectedMonth} para Storage ({releaseId})...[/]");
                await ingestor.ExportAndUploadToStorage(AppConfig.Current.Paths.OutputDir, releaseId);
            }
            else
            {
                AnsiConsole.MarkupLine($"[cyan]4/{totalSteps} Publicando módulos alterados com shards completos ({releaseId})...[/]");
            }

            var moduleShards = await new ModuleShardPublisher().PublishAsync(
                releaseId,
                integrationSummaries,
                publishedInfo,
                AppConfig.Current.Paths.OutputDir);

            AnsiConsole.MarkupLine($"[cyan]5/{totalSteps} Gerando e enviando estatística final...[/]");
            var publication = BuildReleaseInfoPublication(
                ingestor,
                selectedMonth,
                receitaIntegration.Descriptor,
                releaseId,
                receitaChanged,
                publishedInfo,
                publishedLastUpdated,
                integrationSummaries,
                moduleShards);
            await new ReleaseInfoPublisher().PublishAsync(publication, AppConfig.Current.Paths.OutputDir);
            await integrationOrchestrator.PersistStateAsync(integrationSummaries);
        }

        if (settings.CleanupOnSuccess)
        {
            AnsiConsole.MarkupLine($"[cyan]6/{totalSteps} Removendo insumos locais de {selectedMonth}...[/]");
            await LocalArtifactCleaner.CleanupDatasetArtifactsAsync(selectedMonth);
            AnsiConsole.MarkupLine($"[green]✓ Insumos locais de {selectedMonth} removidos[/]");
        }

        AnsiConsole.MarkupLine("[green]✅ Pipeline completo concluído![/]");
        return 0;
    }

    private static int NoWork(string currentMonth)
    {
        AnsiConsole.MarkupLine($"[yellow]ℹ️ A API publicada já está no mês atual ({currentMonth}) e nenhuma integração mudou; nada para fazer.[/]");
        return DatasetPublicationPolicy.NoNewDatasetExitCode;
    }

    private static string ResolveLocalBaseDatasetKey(string? publishedMonth, string currentMonth)
    {
        return DatasetPathResolver.ResolveLatestLocalDatasetKey(AppConfig.Current.Paths)
               ?? publishedMonth
               ?? currentMonth;
    }

    private static DataIntegrationPaths BuildIntegrationPaths()
    {
        var paths = AppConfig.Current.Paths;
        return new DataIntegrationPaths(paths.DataDir, paths.ParquetDir, paths.OutputDir, paths.DownloadDir);
    }

    private static bool RequiresModuleShardBootstrap(
        IReadOnlyList<DataIntegrationRunSummary> integrationSummaries,
        PublishedInfoSnapshot? publishedInfo)
    {
        return DataIntegrationShardSource.FromRunSummaries(integrationSummaries)
            .Any(source => publishedInfo?.ModuleShards.ContainsKey(source.Key) != true);
    }

    private static IReadOnlyDictionary<string, DataIntegrationPublishedState> BuildPublishedIntegrationStates(
        PublishedInfoSnapshot? publishedInfo)
    {
        if (publishedInfo is null)
            return new Dictionary<string, DataIntegrationPublishedState>(StringComparer.Ordinal);

        return publishedInfo.ModuleShards.ToDictionary(
            module => module.Key,
            module => new DataIntegrationPublishedState(
                module.Value.SourceVersion,
                module.Value.UpdatedAt,
                module.Value.SchemaVersion),
            StringComparer.Ordinal);
    }

    private static string ResolveReleaseId(string? requestedReleaseId, string datasetKey)
    {
        if (!string.IsNullOrWhiteSpace(requestedReleaseId))
            return requestedReleaseId.Trim();

        return GenerateReleaseId(datasetKey);
    }

    private static ReleaseInfoPublication BuildReleaseInfoPublication(
        ParquetIngestor ingestor,
        string selectedMonth,
        DataIntegrationDescriptor receitaDescriptor,
        string releaseId,
        bool receitaChanged,
        PublishedInfoSnapshot? publishedInfo,
        string? publishedLastUpdated,
        IReadOnlyList<DataIntegrationRunSummary> integrationSummaries,
        IReadOnlyDictionary<string, ModuleShardPublication> moduleShards)
    {
        if (receitaChanged)
        {
            return new ReleaseInfoPublication(
                ingestor.DatasetKey ?? selectedMonth,
                receitaDescriptor.Key,
                ingestor.CountShardRecordsFromIndexesForPublication(AppConfig.Current.Paths.OutputDir, releaseId),
                ingestor.GetShardCountFromFilesystemForPublication(),
                DateTime.UtcNow.ToString("o"),
                AppConfig.Current.Shards.PrefixLength,
                releaseId,
                releaseId,
                integrationSummaries,
                moduleShards);
        }

        var storageReleaseId = publishedInfo?.StorageReleaseId;
        if (string.IsNullOrWhiteSpace(storageReleaseId)
            || publishedInfo?.Total is null
            || publishedInfo.ShardCount is null)
        {
            throw new InvalidOperationException("Publicação de módulos exige total, shard_count e release base anterior no info.json.");
        }

        return new ReleaseInfoPublication(
            ingestor.DatasetKey ?? selectedMonth,
            receitaDescriptor.Key,
            publishedInfo.Total.Value,
            publishedInfo.ShardCount.Value,
            publishedLastUpdated ?? DateTime.UtcNow.ToString("o"),
            AppConfig.Current.Shards.PrefixLength,
            storageReleaseId,
            releaseId,
            integrationSummaries,
            moduleShards);
    }

    private static string GenerateReleaseId(string datasetKey)
    {
        var seed = $"{datasetKey}|{DateTimeOffset.UtcNow:O}|{Guid.NewGuid():N}";
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(seed));
        return Convert.ToHexString(bytes).ToLowerInvariant()[..16];
    }
}
