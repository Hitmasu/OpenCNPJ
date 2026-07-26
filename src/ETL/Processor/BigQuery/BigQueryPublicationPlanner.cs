using System.Text.RegularExpressions;
using CNPJExporter.Configuration;
using CNPJExporter.Integrations;

namespace CNPJExporter.Processors.BigQuery;

internal static class BigQueryPublicationPlanner
{
    private static readonly Regex BigQueryIdentifierPattern = new(
        "^[A-Za-z_][A-Za-z0-9_]*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static BigQueryPublicationPlan Build(
        AppConfig.BigQuerySettings settings,
        string releaseId,
        IReadOnlyList<BigQueryParquetSource> sources)
    {
        if (!settings.Enabled)
            throw new InvalidOperationException("BigQuery.Enabled=false; o pipeline deve ignorar a publicação BigQuery em vez de montar um plano.");

        var projectId = RequireValue(settings.ProjectId, "BigQuery.ProjectId");
        var dataset = RequireIdentifier(settings.Dataset, "BigQuery.Dataset");
        var tablePrefix = settings.TablePrefix?.Trim() ?? string.Empty;
        if (tablePrefix.Length > 0 && !BigQueryIdentifierPattern.IsMatch(tablePrefix))
            throw new InvalidOperationException("BigQuery.TablePrefix deve ser vazio ou um identificador ASCII simples.");

        var normalizedReleaseId = RequireValue(releaseId, "releaseId");
        var tables = sources
            .Select(source => BuildTablePublication(source, tablePrefix))
            .ToArray();

        return new BigQueryPublicationPlan(
            ProjectId: projectId,
            Dataset: dataset,
            ReleaseId: normalizedReleaseId,
            BqExecutable: string.IsNullOrWhiteSpace(settings.BqExecutable) ? "bq" : settings.BqExecutable.Trim(),
            Location: string.IsNullOrWhiteSpace(settings.Location) ? null : settings.Location.Trim(),
            KeepStagingTables: settings.KeepStagingTables,
            Tables: tables);
    }

    private static BigQueryTablePublication BuildTablePublication(
        BigQueryParquetSource source,
        string tablePrefix)
    {
        var tableName = RequireIdentifier(source.TableName, "BigQuery source table name");
        var sourcePaths = RequireSourcePaths(tableName, source.SourcePaths);

        return new BigQueryTablePublication(
            tableName,
            BuildDestinationTableName(tablePrefix, tableName),
            sourcePaths);
    }

    private static string BuildDestinationTableName(string tablePrefix, string sourceName)
    {
        var tableName = $"{tablePrefix}{sourceName}";
        if (!BigQueryIdentifierPattern.IsMatch(tableName))
            throw new InvalidOperationException($"Tabela BigQuery inválida: {tableName}.");

        return tableName;
    }

    private static string RequireValue(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{name} é obrigatório quando BigQuery.Enabled=true.");

        return value.Trim();
    }

    private static string RequireIdentifier(string? value, string name)
    {
        var normalized = RequireValue(value, name);
        if (!BigQueryIdentifierPattern.IsMatch(normalized))
            throw new InvalidOperationException($"{name} deve ser um identificador ASCII simples.");

        return normalized;
    }

    private static IReadOnlyList<string> RequireSourcePaths(
        string tableName,
        IReadOnlyList<string> sourcePaths)
    {
        if (sourcePaths.Count == 0)
            throw new InvalidOperationException($"Tabela BigQuery {tableName} não possui arquivos Parquet para publicação.");

        return sourcePaths
            .SelectMany((sourcePath, index) =>
                ExpandSourcePath(
                    tableName,
                    RequireValue(
                        sourcePath,
                        $"BigQuery source path {index + 1} for {tableName}")))
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
    }

    private static IReadOnlyList<string> ExpandSourcePath(
        string tableName,
        string sourcePath)
    {
        if (File.Exists(sourcePath))
            return [sourcePath];

        if (!sourcePath.Contains('*', StringComparison.Ordinal)
            && !sourcePath.Contains('?', StringComparison.Ordinal))
        {
            throw new FileNotFoundException(
                $"Parquet da tabela BigQuery {tableName} não encontrado em {sourcePath}.",
                sourcePath);
        }

        var directory = Path.GetDirectoryName(sourcePath);
        var pattern = Path.GetFileName(sourcePath);
        if (string.IsNullOrWhiteSpace(directory)
            || string.IsNullOrWhiteSpace(pattern)
            || !Directory.Exists(directory))
        {
            throw new FileNotFoundException(
                $"Parquet da tabela BigQuery {tableName} não encontrado para o padrão {sourcePath}.",
                sourcePath);
        }

        var matches = Directory
            .EnumerateFiles(directory, pattern, SearchOption.TopDirectoryOnly)
            .Where(path => path.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
            .Order(StringComparer.Ordinal)
            .ToArray();
        if (matches.Length == 0)
        {
            throw new FileNotFoundException(
                $"Parquet da tabela BigQuery {tableName} não encontrado para o padrão {sourcePath}.",
                sourcePath);
        }

        return matches;
    }
}

internal sealed record BigQueryPublicationPlan(
    string ProjectId,
    string Dataset,
    string ReleaseId,
    string BqExecutable,
    string? Location,
    bool KeepStagingTables,
    IReadOnlyList<BigQueryTablePublication> Tables);

internal sealed record BigQueryTablePublication(
    string SourceName,
    string DestinationTableName,
    IReadOnlyList<string> SourcePaths);
