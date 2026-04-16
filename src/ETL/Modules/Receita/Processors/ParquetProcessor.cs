using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Modules.Receita.Processors;

public sealed class ParquetProcessor
{
    private readonly string _dataDir;
    private readonly string _parquetDir;
    private readonly int _shardPrefixLength;

    public ParquetProcessor(string dataDir, string parquetDir, int shardPrefixLength)
    {
        _dataDir = dataDir;
        _parquetDir = parquetDir;
        _shardPrefixLength = Math.Max(1, shardPrefixLength);
    }

    public async Task ConvertCsvsToParquetAsync(DuckDBConnection connection)
    {
        await AnsiConsole.Progress()
            .StartAsync(async ctx =>
            {
                foreach (var (tableName, (pattern, columns)) in TableSchemas.CsvTables)
                {
                    var task = ctx.AddTask($"[green]Processando {tableName}[/]");
                    var files = Directory.GetFiles(_dataDir, pattern, SearchOption.AllDirectories);

                    if (files.Length == 0)
                    {
                        AnsiConsole.MarkupLine($"[yellow]Nenhum arquivo encontrado para {tableName} ({pattern})[/]");
                        task.Increment(100);
                        continue;
                    }

                    task.Description = $"[green]Processando {tableName} ({files.Length} arquivo(s))[/]";
                    task.MaxValue = files.Length;
                    await ConvertTableToParquetAsync(connection, tableName, files, columns, task);
                }
            });
    }

    public async Task LoadTablesForConnectionAsync(
        DuckDBConnection connection,
        bool includeShardTables = true,
        bool showWarnings = true)
    {
        var tableConfigs = new Dictionary<string, string>(StringComparer.Ordinal);

        if (includeShardTables)
        {
            tableConfigs["empresa"] = "empresa/**/*.parquet";
            tableConfigs["estabelecimento"] = "estabelecimento/**/*.parquet";
            tableConfigs["socio"] = "socio/**/*.parquet";
            tableConfigs["simples"] = "simples/**/*.parquet";
        }

        foreach (var (tableName, pattern) in TableSchemas.AuxiliaryTableGlobs)
            tableConfigs[tableName] = pattern;

        foreach (var (tableName, pattern) in tableConfigs)
        {
            try
            {
                var fullPath = Path.Combine(_parquetDir, pattern);
                var createViewSql = TableSchemas.PartitionedTables.Contains(tableName)
                    ? $"CREATE OR REPLACE VIEW {tableName} AS SELECT * FROM read_parquet('{Sql.EscapeLiteral(fullPath)}', hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})"
                    : $"CREATE OR REPLACE VIEW {tableName} AS SELECT * FROM read_parquet('{Sql.EscapeLiteral(fullPath)}')";

                await using var cmd = connection.CreateCommand();
                cmd.CommandText = createViewSql;
                await cmd.ExecuteNonQueryAsync();

                if (showWarnings)
                    AnsiConsole.MarkupLine($"[green]✓ Tabela {tableName} carregada[/]");
            }
            catch (Exception ex)
            {
                if (showWarnings)
                    AnsiConsole.MarkupLine($"[yellow]Aviso ao carregar {tableName}: {ex.Message.EscapeMarkup()}[/]");
            }
        }
    }

    private async Task ConvertTableToParquetAsync(
        DuckDBConnection connection,
        string tableName,
        string[] csvFiles,
        string[] columns,
        ProgressTask task)
    {
        var parquetPath = Path.Combine(_parquetDir, $"{tableName}.parquet");
        var partitionedDir = Path.Combine(_parquetDir, tableName);
        var hasExistingParquet = TableSchemas.PartitionedTables.Contains(tableName)
            ? Directory.Exists(partitionedDir) && Directory.EnumerateFiles(partitionedDir, "*.parquet", SearchOption.AllDirectories).Any()
            : File.Exists(parquetPath);

        if (hasExistingParquet)
        {
            task.Value = task.MaxValue;
            AnsiConsole.MarkupLine($"[yellow]Pulando {tableName}: Parquet já existe[/]");
            return;
        }

        if (TableSchemas.PartitionedTables.Contains(tableName))
        {
            await RecreateDirectoryAsync(partitionedDir);

            for (var index = 0; index < csvFiles.Length; index++)
            {
                var csvFile = csvFiles[index];
                var sourceSql = BuildCsvSourceRelationSql([csvFile], columns);
                var exportSql = $@"
                    COPY (
                        SELECT *,
                               SUBSTRING(cnpj_basico, 1, {_shardPrefixLength}) as cnpj_prefix
                        FROM {sourceSql} AS src
                    )
                    TO '{Sql.EscapeLiteral(partitionedDir)}'
                    (
                        FORMAT PARQUET,
                        COMPRESSION ZSTD,
                        PARTITION_BY (cnpj_prefix),
                        APPEND,
                        FILENAME_PATTERN 'chunk_{index:D3}_{{uuid}}'
                    )";

                await using var exportCmd = connection.CreateCommand();
                exportCmd.CommandText = exportSql;
                await exportCmd.ExecuteNonQueryAsync();
                task.Increment(1);
            }

            AnsiConsole.MarkupLine($"[green]✓ {tableName} convertido para Parquet particionado por cnpj_prefix[/]");
        }
        else
        {
            DeleteIfExists(parquetPath);
            var sourceSql = BuildCsvSourceRelationSql(csvFiles, columns);
            var exportSql = $@"
                COPY (
                    SELECT *
                    FROM {sourceSql} AS src
                )
                TO '{Sql.EscapeLiteral(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";

            await using var exportCmd = connection.CreateCommand();
            exportCmd.CommandText = exportSql;
            await exportCmd.ExecuteNonQueryAsync();

            task.Value = task.MaxValue;
            AnsiConsole.MarkupLine($"[green]✓ {tableName}.parquet criado[/]");
        }
    }

    private static string BuildCsvSourceRelationSql(IEnumerable<string> csvFiles, IReadOnlyList<string> columns)
    {
        var fileListSql = string.Join(", ", csvFiles.Select(file => $"'{Sql.EscapeLiteral(file)}'"));
        var columnsSql = string.Join(", ", columns.Select(column => $"'{Sql.EscapeLiteral(column)}': 'VARCHAR'"));

        return $@"read_csv([{fileListSql}],
                    auto_detect=false,
                    sep=';',
                    header=false,
                    encoding='CP1252',
                    ignore_errors=true,
                    parallel=false,
                    max_line_size=10000000,
                    columns={{{columnsSql}}})";
    }

    private static async Task RecreateDirectoryAsync(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);

        Directory.CreateDirectory(path);
        await Task.CompletedTask;
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
