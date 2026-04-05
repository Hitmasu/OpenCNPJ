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
    [Description("Mês (YYYY-MM). Padrão: mês anterior")]
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

        AnsiConsole.MarkupLine($"[cyan]1/{totalSteps} Baixando dados da Receita...[/]");
        var downloader = new WebDownloader(AppConfig.Current.Paths.DownloadDir, AppConfig.Current.Paths.DataDir);
        var selectedMonth = await downloader.DownloadAndExtractAsync(settings.Month);
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
