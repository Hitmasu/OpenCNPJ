using System.ComponentModel;
using CNPJExporter.Configuration;
using CNPJExporter.Processors;
using CNPJExporter.Utils;
using Spectre.Console;
using Spectre.Console.Cli;

namespace CNPJExporter.Commands;

public class SingleSettings : CommandSettings
{
    [CommandOption("--cnpj|-c")]
    [Description("CNPJ (14 dígitos)")]
    public string? Cnpj { get; init; }

    public override ValidationResult Validate()
    {
        if (!CnpjUtils.IsValidFormat(Cnpj))
            return ValidationResult.Error("Informe --cnpj com 14 dígitos.");
        return ValidationResult.Success();
    }
}

public sealed class SingleCommand : AsyncCommand<SingleSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, SingleSettings settings)
    {
        using var ingestor = new ParquetIngestor();
        var cnpj = settings.Cnpj!;
        AnsiConsole.MarkupLine($"[yellow]🎯 Processando CNPJ {cnpj}[/]");
        await ingestor.ExportSingleCnpjAsync(cnpj, AppConfig.Current.Paths.OutputDir);
        AnsiConsole.MarkupLine("[green]✅ CNPJ processado[/]");
        return 0;
    }
}
