using System.ComponentModel;
using CNPJExporter.Configuration;
using CNPJExporter.Downloaders;
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
        var totalSteps = settings.CleanupOnSuccess ? 5 : 4;
        var currentMonth = DatasetPublicationPolicy.GetCurrentMonth();

        if (string.IsNullOrWhiteSpace(settings.Month))
        {
            var publishedInfoClient = new PublishedInfoClient();
            var publishedLastUpdated = await publishedInfoClient.GetPublishedLastUpdatedAsync();
            if (DatasetPublicationPolicy.TryGetPublishedMonth(publishedLastUpdated, out var publishedMonth)
                && string.Equals(publishedMonth, currentMonth, StringComparison.Ordinal))
            {
                AnsiConsole.MarkupLine($"[yellow]ℹ️ A API publicada já está no mês atual ({publishedMonth}); nada para fazer.[/]");
                return DatasetPublicationPolicy.NoNewDatasetExitCode;
            }
        }

        AnsiConsole.MarkupLine($"[cyan]1/{totalSteps} Baixando dados da Receita...[/]");
        var downloader = new WebDownloader(AppConfig.Current.Paths.DownloadDir, AppConfig.Current.Paths.DataDir);
        var availableMonths = await downloader.GetAvailableMonthsAsync();

        if (!DatasetPublicationPolicy.TrySelectMonthToProcess(settings.Month, availableMonths, out var selectedMonth, out var latestAvailableMonth))
        {
            AnsiConsole.MarkupLine(
                $"[yellow]ℹ️ Nenhuma base nova para publicar. Último mês disponível na Receita: {latestAvailableMonth}. Mês atual esperado: {currentMonth}[/]");
            return DatasetPublicationPolicy.NoNewDatasetExitCode;
        }

        selectedMonth = await downloader.DownloadAndExtractAsync(selectedMonth, availableMonths);
        var releaseId = string.IsNullOrWhiteSpace(settings.ReleaseId)
            ? GenerateReleaseId(selectedMonth)
            : settings.ReleaseId.Trim();

        AnsiConsole.MarkupLine($"[grey]Release id:[/] [cyan]{releaseId.EscapeMarkup()}[/]");

        using (var ingestor = new ParquetIngestor(selectedMonth))
        {
            AnsiConsole.MarkupLine($"[cyan]2/{totalSteps} Convertendo CSVs de {selectedMonth} para Parquet...[/]");
            await ingestor.ConvertCsvsToParquet();

            AnsiConsole.MarkupLine($"[cyan]3/{totalSteps} Gerando shards e enviando {selectedMonth} para Storage ({releaseId})...[/]");
            await ingestor.ExportAndUploadToStorage(AppConfig.Current.Paths.OutputDir, releaseId);

            AnsiConsole.MarkupLine($"[cyan]4/{totalSteps} Gerando e enviando estatística final...[/]");
            await ingestor.GenerateAndUploadFinalInfoJsonAsync(releaseId);
        }

        if (settings.CleanupOnSuccess)
        {
            AnsiConsole.MarkupLine($"[cyan]5/{totalSteps} Removendo artefatos locais de {selectedMonth}...[/]");
            await LocalArtifactCleaner.CleanupDatasetArtifactsAsync(selectedMonth);
            AnsiConsole.MarkupLine($"[green]✓ Artefatos locais de {selectedMonth} removidos[/]");
        }

        AnsiConsole.MarkupLine("[green]✅ Pipeline completo concluído![/]");
        return 0;
    }

    private static string GenerateReleaseId(string datasetKey)
    {
        var seed = $"{datasetKey}|{DateTimeOffset.UtcNow:O}|{Guid.NewGuid():N}";
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(seed));
        return Convert.ToHexString(bytes).ToLowerInvariant()[..16];
    }
}
