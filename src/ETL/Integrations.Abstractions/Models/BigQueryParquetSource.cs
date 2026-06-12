namespace CNPJExporter.Integrations;

public sealed record BigQueryParquetSource(
    string TableName,
    IReadOnlyList<string> SourcePaths);
