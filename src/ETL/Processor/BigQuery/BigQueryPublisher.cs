using System.Text.RegularExpressions;
using Spectre.Console;

namespace CNPJExporter.Processors.BigQuery;

internal sealed class BigQueryPublisher
{
    private static readonly Regex InvalidTableNameCharacterPattern = new("[^A-Za-z0-9_]", RegexOptions.Compiled);
    private readonly IBigQueryCommandRunner _commandRunner;

    public BigQueryPublisher(IBigQueryCommandRunner? commandRunner = null)
    {
        _commandRunner = commandRunner ?? new ProcessBigQueryCommandRunner();
    }

    public async Task PublishAsync(
        BigQueryPublicationPlan plan,
        CancellationToken cancellationToken = default)
    {
        if (plan.Tables.Count == 0)
        {
            AnsiConsole.MarkupLine("[yellow]BigQuery habilitado, mas nenhuma tabela foi selecionada para publicação.[/]");
            return;
        }

        AnsiConsole.MarkupLine(
            $"[cyan]Atualizando BigQuery[/] [grey](dataset: {plan.Dataset.EscapeMarkup()}, tabelas: {plan.Tables.Count})[/]");

        var preparedTables = new List<PreparedBigQueryTable>(plan.Tables.Count);
        foreach (var table in plan.Tables)
        {
            if (table.SourcePaths.Count == 0)
                throw new InvalidOperationException($"Tabela BigQuery {table.SourceName} não possui arquivos Parquet para publicação.");

            foreach (var sourcePath in table.SourcePaths)
            {
                if (!File.Exists(sourcePath))
                    throw new FileNotFoundException($"Arquivo Parquet para BigQuery não encontrado: {sourcePath}.", sourcePath);
            }

            preparedTables.Add(new PreparedBigQueryTable(
                table,
                BuildStagingTableName(table.DestinationTableName, plan.ReleaseId)));
        }

        await ValidateTargetDatasetAsync(plan, cancellationToken);

        foreach (var table in preparedTables)
        {
            AnsiConsole.MarkupLine(
                $"[grey]BigQuery:[/] carregando staging [cyan]{table.StagingTableName.EscapeMarkup()}[/] [grey]({table.Source.SourcePaths.Count} parte(s))[/]");

            for (var index = 0; index < table.Source.SourcePaths.Count; index++)
            {
                var sourcePath = table.Source.SourcePaths[index];
                var loadArguments = new List<string>
                {
                    "load"
                };
                if (index == 0)
                    loadArguments.Add("--replace");

                loadArguments.Add("--source_format=PARQUET");
                loadArguments.Add(FormatTableSpec(plan, table.StagingTableName));
                loadArguments.Add(sourcePath);

                AnsiConsole.MarkupLine(
                    $"[grey]BigQuery:[/] parte {index + 1}/{table.Source.SourcePaths.Count} [grey]{Path.GetFileName(sourcePath).EscapeMarkup()}[/]");
                await RunBqAsync(plan, loadArguments, cancellationToken);
            }
        }

        foreach (var table in preparedTables)
        {
            AnsiConsole.MarkupLine(
                $"[grey]BigQuery:[/] substituindo tabela final [cyan]{table.Source.DestinationTableName.EscapeMarkup()}[/]");
            await RunBqAsync(
                plan,
                [
                    "cp",
                    "--force",
                    FormatTableSpec(plan, table.StagingTableName),
                    FormatTableSpec(plan, table.Source.DestinationTableName)
                ],
                cancellationToken);
        }

        if (!plan.KeepStagingTables)
        {
            foreach (var table in preparedTables)
            {
                await RunBqAsync(
                    plan,
                    [
                        "rm",
                        "--force",
                        "--table",
                        FormatTableSpec(plan, table.StagingTableName)
                    ],
                    cancellationToken);
            }
        }

        AnsiConsole.MarkupLine("[green]✓ BigQuery atualizado[/]");
    }

    internal static string BuildStagingTableNameForTest(string destinationTableName, string releaseId) =>
        BuildStagingTableName(destinationTableName, releaseId);

    private async Task RunBqAsync(
        BigQueryPublicationPlan plan,
        IReadOnlyList<string> commandArguments,
        CancellationToken cancellationToken)
    {
        var arguments = new List<string>();
        if (!string.IsNullOrWhiteSpace(plan.Location))
            arguments.Add($"--location={plan.Location}");

        arguments.AddRange(commandArguments);
        await _commandRunner.RunAsync(plan.BqExecutable, arguments, cancellationToken);
    }

    private async Task ValidateTargetDatasetAsync(
        BigQueryPublicationPlan plan,
        CancellationToken cancellationToken)
    {
        AnsiConsole.MarkupLine(
            $"[grey]BigQuery:[/] validando autenticação e dataset [cyan]{FormatDatasetSpec(plan).EscapeMarkup()}[/]");
        await RunBqAsync(
            plan,
            [
                "show",
                "--format=none",
                FormatDatasetSpec(plan)
            ],
            cancellationToken);
    }

    private static string FormatDatasetSpec(BigQueryPublicationPlan plan) =>
        $"{plan.ProjectId}:{plan.Dataset}";

    private static string FormatTableSpec(BigQueryPublicationPlan plan, string tableName) =>
        $"{plan.ProjectId}:{plan.Dataset}.{tableName}";

    private static string BuildStagingTableName(string destinationTableName, string releaseId)
    {
        var suffix = InvalidTableNameCharacterPattern.Replace(releaseId.Trim('/'), "_").Trim('_');
        if (string.IsNullOrWhiteSpace(suffix))
            suffix = "release";

        return $"{destinationTableName}__staging_{suffix}";
    }

    private sealed record PreparedBigQueryTable(
        BigQueryTablePublication Source,
        string StagingTableName);
}
