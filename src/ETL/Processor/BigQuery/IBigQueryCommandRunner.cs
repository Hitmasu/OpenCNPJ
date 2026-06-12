namespace CNPJExporter.Processors.BigQuery;

internal interface IBigQueryCommandRunner
{
    Task RunAsync(
        string executable,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken = default);
}
