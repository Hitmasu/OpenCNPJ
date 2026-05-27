using System.Globalization;
using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Modules.Receita.Processors;

public sealed class ParquetProcessor
{
    private const int CnpjBasicoLength = 8;
    private const int CnpjBasicoMaxExclusive = 100_000_000;
    private const int DefaultQsaMaterializationRangeFanOut = 2;
    private const string QsaTableName = "qsa";
    private static readonly IReadOnlySet<string> QsaDependencyTables = new HashSet<string>(StringComparer.Ordinal)
    {
        "socio",
        "pais",
        "qualificacao"
    };
    private readonly string _dataDir;
    private readonly string _parquetDir;
    private readonly int _shardPrefixLength;

    public ParquetProcessor(string dataDir, string parquetDir, int shardPrefixLength)
    {
        _dataDir = dataDir;
        _parquetDir = parquetDir;
        _shardPrefixLength = Math.Max(1, shardPrefixLength);
    }

    public async Task ConvertCsvsToParquetAsync(
        DuckDBConnection connection,
        int qsaMaterializationRangeFanOut = DefaultQsaMaterializationRangeFanOut)
    {
        var qsaInputsChanged = false;

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
                    var converted = await ConvertTableToParquetAsync(connection, tableName, files, columns, task);
                    qsaInputsChanged |= converted && QsaDependencyTables.Contains(tableName);
                }
            });

        await CompactPartitionedParquetAsync(connection, TableSchemas.PartitionedTables);
        await MaterializeQsaParquetAsync(connection, qsaMaterializationRangeFanOut, forceRecreate: qsaInputsChanged);
        await CompactPartitionedParquetAsync(connection, [QsaTableName]);
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

    private async Task<bool> ConvertTableToParquetAsync(
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
            return false;
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
            return true;
        }

        DeleteIfExists(parquetPath);
        var nonPartitionedSourceSql = BuildCsvSourceRelationSql(csvFiles, columns);
        var nonPartitionedExportSql = $@"
                COPY (
                    SELECT *
                    FROM {nonPartitionedSourceSql} AS src
                )
                TO '{Sql.EscapeLiteral(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";

        await using var nonPartitionedExportCmd = connection.CreateCommand();
        nonPartitionedExportCmd.CommandText = nonPartitionedExportSql;
        await nonPartitionedExportCmd.ExecuteNonQueryAsync();

        task.Value = task.MaxValue;
        AnsiConsole.MarkupLine($"[green]✓ {tableName}.parquet criado[/]");
        return true;
    }

    private async Task CompactPartitionedParquetAsync(
        DuckDBConnection connection,
        IEnumerable<string> tableNames)
    {
        var tablePartitions = tableNames
            .Select(tableName => (TableName: tableName, TableDir: Path.Combine(_parquetDir, tableName)))
            .Where(table => Directory.Exists(table.TableDir))
            .Select(table => (
                table.TableName,
                PartitionDirs: Directory
                    .EnumerateDirectories(table.TableDir, "cnpj_prefix=*", SearchOption.TopDirectoryOnly)
                    .Where(HasMultipleParquetFiles)
                    .OrderBy(static path => path, StringComparer.Ordinal)
                    .ToArray()))
            .Where(table => table.PartitionDirs.Length > 0)
            .ToArray();

        if (tablePartitions.Length == 0)
            return;

        AnsiConsole.MarkupLine("[cyan]Compactando partições Parquet pequenas antes dos shards...[/]");

        await AnsiConsole.Progress()
            .StartAsync(async ctx =>
            {
                foreach (var (tableName, partitionDirs) in tablePartitions)
                {
                    var task = ctx.AddTask(
                        $"[green]Compactando {tableName}[/]",
                        maxValue: partitionDirs.Length);

                    foreach (var partitionDir in partitionDirs)
                    {
                        await CompactPartitionAsync(connection, tableName, partitionDir);
                        task.Increment(1);
                    }
                }
            });
    }

    public async Task MaterializeQsaParquetAsync(
        DuckDBConnection connection,
        int rangeFanOut = DefaultQsaMaterializationRangeFanOut,
        bool forceRecreate = false)
    {
        var socioDir = Path.Combine(_parquetDir, "socio");
        if (!Directory.Exists(socioDir)
            || !Directory.EnumerateFiles(socioDir, "*.parquet", SearchOption.AllDirectories).Any())
        {
            return;
        }

        var qsaDir = Path.Combine(_parquetDir, QsaTableName);
        if (!forceRecreate && HasPartitionedParquet(qsaDir))
            return;

        var qualificacaoPath = Path.Combine(_parquetDir, "qualificacao.parquet");
        var paisPath = Path.Combine(_parquetDir, "pais.parquet");
        if (!File.Exists(qualificacaoPath) || !File.Exists(paisPath))
            throw new InvalidOperationException("QSA materializado exige qualificacao.parquet e pais.parquet.");

        var prefixes = GetPartitionPrefixes(socioDir);
        if (prefixes.Length == 0)
            return;

        var tempRoot = Path.Combine(_parquetDir, $".{QsaTableName}-tmp-{Guid.NewGuid():N}");
        await RecreateDirectoryAsync(tempRoot);
        try
        {
            AnsiConsole.MarkupLine("[cyan]Materializando QSA enriquecido para acelerar shards...[/]");

            await AnsiConsole.Progress()
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask("[green]Materializando QSA[/]", maxValue: prefixes.Length);
                    for (var prefixIndex = 0; prefixIndex < prefixes.Length; prefixIndex++)
                    {
                        var prefix = prefixes[prefixIndex];
                        var ranges = BuildInitialRanges(prefix, rangeFanOut);
                        for (var rangeIndex = 0; rangeIndex < ranges.Count; rangeIndex++)
                        {
                            await MaterializeQsaRangeAsync(
                                connection,
                                prefix,
                                ranges[rangeIndex],
                                tempRoot,
                                qualificacaoPath,
                                paisPath,
                                rangeFanOut,
                                prefixIndex,
                                rangeIndex);
                        }

                        task.Increment(1);
                    }
                });

            if (Directory.Exists(qsaDir))
                Directory.Delete(qsaDir, recursive: true);

            Directory.Move(tempRoot, qsaDir);
        }
        catch
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);

            throw;
        }
    }

    private async Task MaterializeQsaRangeAsync(
        DuckDBConnection connection,
        string prefix,
        CnpjBasicoRange? range,
        string outputDir,
        string qualificacaoPath,
        string paisPath,
        int rangeFanOut,
        int prefixIndex,
        int rangeIndex)
    {
        try
        {
            var sourceGlob = Path.Combine(_parquetDir, "socio", $"cnpj_prefix={prefix}", "*.parquet");
            var sourceRelation = BuildParquetReadSql(sourceGlob, hivePartitioning: true);
            var qualificacaoRelation = BuildParquetReadSql(qualificacaoPath, hivePartitioning: false);
            var paisRelation = BuildParquetReadSql(paisPath, hivePartitioning: false);
            var prefixLiteral = Sql.EscapeLiteral(prefix);
            var selectSql = QsaProjection.BuildSelect(
                sourceRelation,
                qualificacaoRelation,
                paisRelation,
                prefixLiteral,
                cnpjBasicoStartInclusive: range?.StartLiteral,
                cnpjBasicoEndExclusive: range?.EndLiteral);
            var fileNamePrefix = $"{prefixIndex:D4}_{rangeIndex:D3}_{SanitizeFileNamePart(prefix)}";
            var copySql = $@"
                COPY (
                    {selectSql}
                )
                TO '{Sql.EscapeLiteral(outputDir)}'
                (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD,
                    PARTITION_BY (cnpj_prefix),
                    APPEND,
                    FILENAME_PATTERN 'qsa_{fileNamePrefix}_{{uuid}}'
                )";

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = copySql;
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex) when (range is { CanSplit: true } failedRange && IsDuckDbOutOfMemory(ex))
        {
            AnsiConsole.MarkupLine(
                $"[yellow]DuckDB excedeu memória no QSA {prefix} faixa {failedRange.ToString().EscapeMarkup()}; subdividindo.[/]");

            var childIndex = 0;
            foreach (var childRange in SplitRange(failedRange, NormalizeRangeFanOut(rangeFanOut)))
            {
                await MaterializeQsaRangeAsync(
                    connection,
                    prefix,
                    childRange,
                    outputDir,
                    qualificacaoPath,
                    paisPath,
                    rangeFanOut,
                    prefixIndex,
                    rangeIndex * 100 + childIndex++);
            }
        }
    }

    private static string[] GetPartitionPrefixes(string tableDir) =>
        Directory
            .EnumerateDirectories(tableDir, "cnpj_prefix=*", SearchOption.TopDirectoryOnly)
            .Select(Path.GetFileName)
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Select(static name => name!["cnpj_prefix=".Length..])
            .Where(static prefix => !string.IsNullOrWhiteSpace(prefix))
            .OrderBy(static prefix => prefix, StringComparer.Ordinal)
            .ToArray();

    private static IReadOnlyList<CnpjBasicoRange?> BuildInitialRanges(string prefix, int fanOut)
    {
        if (!TryBuildFullCnpjBasicoRange(prefix, out var fullRange))
            return [null];

        return SplitRange(fullRange, NormalizeRangeFanOut(fanOut))
            .Select(static range => (CnpjBasicoRange?)range)
            .ToArray();
    }

    private static bool TryBuildFullCnpjBasicoRange(string prefix, out CnpjBasicoRange range)
    {
        range = default;

        if (prefix.Length > CnpjBasicoLength
            || !int.TryParse(prefix, NumberStyles.None, CultureInfo.InvariantCulture, out var prefixNumber))
        {
            return false;
        }

        var multiplier = Pow10(CnpjBasicoLength - prefix.Length);
        var startInclusive = prefixNumber * multiplier;
        var endExclusive = Math.Min(CnpjBasicoMaxExclusive, startInclusive + multiplier);

        range = new CnpjBasicoRange(startInclusive, endExclusive);
        return true;
    }

    private static IReadOnlyList<CnpjBasicoRange> SplitRange(CnpjBasicoRange range, int fanOut)
    {
        var width = range.EndExclusive - range.StartInclusive;
        if (width <= 1)
            return [range];

        var chunkWidth = Math.Max(1, (width + fanOut - 1) / fanOut);
        var ranges = new List<CnpjBasicoRange>();

        for (var start = range.StartInclusive; start < range.EndExclusive; start += chunkWidth)
        {
            var end = Math.Min(range.EndExclusive, start + chunkWidth);
            ranges.Add(new CnpjBasicoRange(start, end));
        }

        return ranges;
    }

    private static int NormalizeRangeFanOut(int fanOut) => Math.Max(2, fanOut);

    private static int Pow10(int exponent)
    {
        var result = 1;
        for (var i = 0; i < exponent; i++)
            result *= 10;
        return result;
    }

    private static bool HasPartitionedParquet(string tableDir) =>
        Directory.Exists(tableDir)
        && Directory.EnumerateFiles(tableDir, "*.parquet", SearchOption.AllDirectories).Any();

    private static bool IsDuckDbOutOfMemory(Exception ex)
    {
        return ex.Message.Contains("Out of Memory", StringComparison.OrdinalIgnoreCase)
               || ex.InnerException is not null && IsDuckDbOutOfMemory(ex.InnerException);
    }

    private static string BuildParquetReadSql(string pathOrGlob, bool hivePartitioning) =>
        hivePartitioning
            ? $"read_parquet('{Sql.EscapeLiteral(pathOrGlob)}', hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})"
            : $"read_parquet('{Sql.EscapeLiteral(pathOrGlob)}')";

    private static string SanitizeFileNamePart(string value)
    {
        var chars = value.ToCharArray();
        for (var index = 0; index < chars.Length; index++)
        {
            if (!char.IsLetterOrDigit(chars[index]))
                chars[index] = '_';
        }

        return new string(chars);
    }

    private readonly record struct CnpjBasicoRange(int StartInclusive, int EndExclusive)
    {
        public string StartLiteral => StartInclusive.ToString($"D{CnpjBasicoLength}", CultureInfo.InvariantCulture);

        public string? EndLiteral => EndExclusive >= CnpjBasicoMaxExclusive
            ? null
            : EndExclusive.ToString($"D{CnpjBasicoLength}", CultureInfo.InvariantCulture);

        public bool CanSplit => EndExclusive - StartInclusive > 1;

        public override string ToString()
        {
            return EndLiteral is null
                ? $"[{StartLiteral}, max]"
                : $"[{StartLiteral}, {EndLiteral})";
        }
    }

    private async Task CompactPartitionAsync(
        DuckDBConnection connection,
        string tableName,
        string partitionDir)
    {
        var tempRoot = Path.Combine(_parquetDir, ".compact-tmp", tableName, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        var tempPath = Path.Combine(tempRoot, "compact.parquet");
        var finalPath = Path.Combine(partitionDir, "compact.parquet");
        var sourceGlob = Path.Combine(partitionDir, "*.parquet");

        try
        {
            var sql = BuildPartitionCompactionSql(sourceGlob, tempPath);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();

            foreach (var parquetFile in Directory.EnumerateFiles(partitionDir, "*.parquet", SearchOption.TopDirectoryOnly))
                File.Delete(parquetFile);

            File.Move(tempPath, finalPath);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    internal static string BuildPartitionCompactionSql(string sourceGlob, string outputPath) =>
        $@"
                COPY (
                    SELECT * EXCLUDE (cnpj_prefix)
                    FROM read_parquet('{Sql.EscapeLiteral(sourceGlob)}', hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})
                )
                TO '{Sql.EscapeLiteral(outputPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";

    private static bool HasMultipleParquetFiles(string partitionDir) =>
        Directory.EnumerateFiles(partitionDir, "*.parquet", SearchOption.TopDirectoryOnly)
            .Take(2)
            .Count() > 1;

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
