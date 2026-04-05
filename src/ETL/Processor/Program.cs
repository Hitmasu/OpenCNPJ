using CNPJExporter.Commands;
using CNPJExporter.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;

AppConfig.Load();

AnsiConsole.MarkupLine("[bold blue]🚀 OpenCNPJ ETL Processor[/]");

var app = new CommandApp();
app.Configure(config =>
{
    config.SetApplicationName("opencnpj-etl");
    config.ValidateExamples();

    config.AddCommand<PipelineCommand>("pipeline").WithDescription("Pipeline principal (download → ingest → shards versionados → info)");
});

return app.Run(args);
