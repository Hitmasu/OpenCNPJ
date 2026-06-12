using CNPJExporter.Integrations;
using CNPJExporter.Modules.Receita.Processors;
using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Modules.Receita;

public sealed class BigQueryParquetExporter
{
    public const string TableName = "receita";

    private const int DefaultPrefixBatchSize = 10;

    private readonly string _parquetRoot;
    private readonly int _shardPrefixLength;

    public BigQueryParquetExporter(string parquetRoot, int shardPrefixLength)
    {
        _parquetRoot = parquetRoot;
        _shardPrefixLength = Math.Max(1, shardPrefixLength);
    }

    public async Task<IReadOnlyList<string>> MaterializeAsync(
        string datasetKey,
        CancellationToken cancellationToken = default)
    {
        var outputDir = GetPartsDirectory(_parquetRoot);
        if (Directory.Exists(outputDir))
            Directory.Delete(outputDir, recursive: true);

        Directory.CreateDirectory(outputDir);

        var shardQueryBuilder = new ShardQueryBuilder(_parquetRoot);
        var prefixes = shardQueryBuilder.GetExistingShardPrefixes();
        if (prefixes.Count == 0)
            throw new InvalidOperationException("Nenhuma partição da Receita encontrada para materializar o BigQuery.");

        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await ConfigureAsync(connection, outputDir, cancellationToken);

        var parquetProcessor = new ParquetProcessor(
            dataDir: string.Empty,
            parquetDir: _parquetRoot,
            shardPrefixLength: _shardPrefixLength);
        await parquetProcessor.LoadTablesForConnectionAsync(
            connection,
            includeShardTables: false,
            showWarnings: false);

        AnsiConsole.MarkupLine(
            $"[grey]BigQuery Receita:[/] materializando {prefixes.Count} prefixos em partes colunares");

        var outputPaths = new List<string>();
        for (var offset = 0; offset < prefixes.Count; offset += DefaultPrefixBatchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var batch = prefixes.Skip(offset).Take(DefaultPrefixBatchSize).ToArray();
            var outputPath = Path.Combine(outputDir, $"part-{offset / DefaultPrefixBatchSize:00000}.parquet");

            var columnarQuery = shardQueryBuilder.BuildColumnarQueryForPrefixBatch(batch);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                COPY (
                    {columnarQuery}
                )
                TO '{EscapeSqlLiteral(outputPath)}'
                (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";
            await cmd.ExecuteNonQueryAsync(cancellationToken);

            outputPaths.Add(outputPath);
            AnsiConsole.MarkupLine(
                $"[grey]BigQuery Receita:[/] parte {outputPaths.Count} gerada [grey]({batch.First().EscapeMarkup()}..{batch.Last().EscapeMarkup()})[/]");
        }

        return outputPaths;
    }

    public static BigQueryParquetSource GetSource(string parquetRoot)
    {
        var partsDir = GetPartsDirectory(parquetRoot);
        IReadOnlyList<string> sourcePaths = Directory.Exists(partsDir)
            ? Directory
                .EnumerateFiles(partsDir, "part-*.parquet", SearchOption.TopDirectoryOnly)
                .Order(StringComparer.Ordinal)
                .ToArray()
            : Array.Empty<string>();

        return new BigQueryParquetSource(TableName, sourcePaths);
    }

    public static string GetPartsDirectory(string parquetRoot) =>
        Path.Combine(parquetRoot, "bigquery", TableName);

    private static async Task ConfigureAsync(
        DuckDBConnection connection,
        string outputDir,
        CancellationToken cancellationToken)
    {
        var tempDir = Path.Combine(outputDir, "_duckdb_temp");
        Directory.CreateDirectory(tempDir);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SET threads = 4;
            SET memory_limit = '8GB';
            SET preserve_insertion_order = false;
            SET temp_directory = '{EscapeSqlLiteral(tempDir)}';
            SET max_temp_directory_size = '200GB';";
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
