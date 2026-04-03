using System.ComponentModel;
using CNPJExporter.Configuration;
using CNPJExporter.Downloaders;
using CNPJExporter.Processors;
using CNPJExporter.Utils;
using Spectre.Console;
using Spectre.Console.Cli;

namespace CNPJExporter.Commands;

public class PipelineSettings : CommandSettings
{
    [CommandOption("--month|-m")]
    [Description("Mês (YYYY-MM). Padrão: mês anterior")]
    public string? Month { get; init; }

    [CommandOption("--cleanup-on-success")]
    [Description("Remove artefatos locais do dataset após processamento concluído com sucesso")]
    public bool CleanupOnSuccess { get; init; }
}

public sealed class PipelineCommand : AsyncCommand<PipelineSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, PipelineSettings settings)
    {
        var totalSteps = settings.CleanupOnSuccess ? 6 : 5;

        AnsiConsole.MarkupLine($"[cyan]1/{totalSteps} Baixando dados da Receita...[/]");
        var downloader = new WebDownloader(AppConfig.Current.Paths.DownloadDir, AppConfig.Current.Paths.DataDir);
        var selectedMonth = await downloader.DownloadAndExtractAsync(settings.Month);

        using (var ingestor = new ParquetIngestor(selectedMonth))
        {
            AnsiConsole.MarkupLine($"[cyan]2/{totalSteps} Convertendo CSVs de {selectedMonth} para Parquet...[/]");
            await ingestor.ConvertCsvsToParquet();

            AnsiConsole.MarkupLine($"[cyan]3/{totalSteps} Gerando shards e enviando {selectedMonth} para Storage...[/]");
            await ingestor.ExportAndUploadToStorage(AppConfig.Current.Paths.OutputDir);

            AnsiConsole.MarkupLine($"[cyan]4/{totalSteps} Gerando e enviando estatística final...[/]");
            await ingestor.GenerateAndUploadFinalInfoJsonAsync();

            AnsiConsole.MarkupLine($"[cyan]5/{totalSteps} Preparando índices e info para Static Assets do Worker...[/]");
            var stagedAssetsPath = await WorkerAssetStager.StageAsync(selectedMonth);
            AnsiConsole.MarkupLine($"[green]✓ Static Assets preparados em[/] [grey]{stagedAssetsPath.EscapeMarkup()}[/]");
        }

        if (settings.CleanupOnSuccess)
        {
            AnsiConsole.MarkupLine($"[cyan]6/{totalSteps} Removendo artefatos locais de {selectedMonth}...[/]");
            await LocalArtifactCleaner.CleanupDatasetArtifactsAsync(selectedMonth);
            AnsiConsole.MarkupLine($"[green]✓ Artefatos locais de {selectedMonth} removidos[/]");
        }

        AnsiConsole.MarkupLine("[green]✅ Pipeline completo concluído![/]");
        return 0;
    }
}
