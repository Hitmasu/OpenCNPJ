using System.Security.Cryptography;
using System.Text;
using CNPJExporter.Configuration;
using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Processors.BigQuery;

internal static class BigQueryParquetCompactor
{
    internal static BigQueryParquetCompactorSettings ResolveSettings(
        AppConfig config)
    {
        var settings = config.BigQuery;
        if (settings.CompactionThreads <= 0)
        {
            throw new InvalidOperationException(
                "BigQuery.CompactionThreads deve ser maior que zero.");
        }

        if (string.IsNullOrWhiteSpace(settings.CompactionMemoryLimit))
        {
            throw new InvalidOperationException(
                "BigQuery.CompactionMemoryLimit é obrigatório.");
        }

        if (string.IsNullOrWhiteSpace(
                settings.CompactionMaxTempDirectorySize))
        {
            throw new InvalidOperationException(
                "BigQuery.CompactionMaxTempDirectorySize é obrigatório.");
        }

        return new BigQueryParquetCompactorSettings(
            settings.CompactionThreads,
            settings.CompactionMemoryLimit,
            settings.CompactionMaxTempDirectorySize);
    }

    public static async Task<IReadOnlyList<string>> MaterializeAsync(
        string tableName,
        IReadOnlyList<string> sourcePaths,
        string parquetRoot,
        BigQueryParquetCompactorSettings settings,
        CancellationToken cancellationToken = default)
    {
        var results = new List<string>(sourcePaths.Count);
        foreach (var sourcePath in sourcePaths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!ContainsWildcard(sourcePath))
            {
                results.Add(sourcePath);
                continue;
            }

            results.Add(
                await CompactGlobAsync(
                    tableName,
                    sourcePath,
                    parquetRoot,
                    settings,
                    cancellationToken));
        }

        return results;
    }

    private static async Task<string> CompactGlobAsync(
        string tableName,
        string sourceGlob,
        string parquetRoot,
        BigQueryParquetCompactorSettings settings,
        CancellationToken cancellationToken)
    {
        var sourceFiles = ExpandGlob(sourceGlob);
        if (sourceFiles.Count == 0)
        {
            throw new FileNotFoundException(
                $"Nenhum Parquet encontrado para compactação BigQuery: {sourceGlob}.",
                sourceGlob);
        }

        var sourceKey = Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(sourceGlob)))
            .ToLowerInvariant()[..16];
        var outputDirectory = Path.Combine(
            parquetRoot,
            "bigquery",
            "integrations",
            tableName);
        var outputPath = Path.Combine(outputDirectory, $"{sourceKey}.parquet");
        var newestSourceWrite = sourceFiles
            .Select(File.GetLastWriteTimeUtc)
            .Max();
        if (File.Exists(outputPath)
            && File.GetLastWriteTimeUtc(outputPath) >= newestSourceWrite)
        {
            return outputPath;
        }

        Directory.CreateDirectory(outputDirectory);
        var tempDirectory = Path.Combine(outputDirectory, "_duckdb_temp");
        Directory.CreateDirectory(tempDirectory);
        var temporaryOutputPath = $"{outputPath}.tmp";
        DeleteIfExists(temporaryOutputPath);

        AnsiConsole.MarkupLine(
            $"[grey]BigQuery {tableName.EscapeMarkup()}:[/] compactando [cyan]{sourceFiles.Count:N0}[/] partes em [grey]{outputPath.EscapeMarkup()}[/]");

        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync(cancellationToken);
        await using (var configure = connection.CreateCommand())
        {
            configure.CommandText = $"""
                SET preserve_insertion_order = false;
                SET threads = {settings.Threads};
                SET memory_limit = '{EscapeSqlLiteral(settings.MemoryLimit)}';
                SET temp_directory = '{EscapeSqlLiteral(tempDirectory)}';
                SET max_temp_directory_size = '{EscapeSqlLiteral(settings.MaxTempDirectorySize)}';
                """;
            await configure.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var compact = connection.CreateCommand())
        {
            compact.CommandText = $"""
                COPY (
                    SELECT *
                    FROM read_parquet('{EscapeSqlLiteral(sourceGlob)}')
                )
                TO '{EscapeSqlLiteral(temporaryOutputPath)}'
                (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD,
                    ROW_GROUP_SIZE 100000,
                    OVERWRITE
                )
                """;
            await compact.ExecuteNonQueryAsync(cancellationToken);
        }

        File.Move(temporaryOutputPath, outputPath, overwrite: true);
        return outputPath;
    }

    private static IReadOnlyList<string> ExpandGlob(string sourceGlob)
    {
        var directory = Path.GetDirectoryName(sourceGlob);
        var pattern = Path.GetFileName(sourceGlob);
        if (string.IsNullOrWhiteSpace(directory)
            || string.IsNullOrWhiteSpace(pattern)
            || !Directory.Exists(directory))
        {
            return [];
        }

        return Directory
            .EnumerateFiles(directory, pattern, SearchOption.TopDirectoryOnly)
            .Where(path => path.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
            .Order(StringComparer.Ordinal)
            .ToArray();
    }

    private static bool ContainsWildcard(string path) =>
        path.Contains('*', StringComparison.Ordinal)
        || path.Contains('?', StringComparison.Ordinal);

    private static string EscapeSqlLiteral(string value) =>
        value.Replace("'", "''", StringComparison.Ordinal);

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}

internal sealed record BigQueryParquetCompactorSettings(
    int Threads,
    string MemoryLimit,
    string MaxTempDirectorySize);
