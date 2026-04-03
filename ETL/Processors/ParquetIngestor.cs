using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using CNPJExporter.Configuration;
using CNPJExporter.Exporters;
using CNPJExporter.Utils;
using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Processors;

public class ParquetIngestor : IDisposable
{
    private const string ShardDataExtension = ".ndjson";
    private const string ShardIndexExtension = ".index.json";
    private readonly string? _datasetKey;
    private readonly string _dataDir;
    private readonly string _parquetDir;
    private readonly string _dataSource;
    private readonly DuckDBConnection _connection;

    public ParquetIngestor(string? datasetKey = null)
    {
        _datasetKey = ResolveDatasetKey(datasetKey);
        _dataDir = DatasetPathResolver.GetDatasetPath(AppConfig.Current.Paths.DataDir, _datasetKey);
        _parquetDir = DatasetPathResolver.GetDatasetPath(AppConfig.Current.Paths.ParquetDir, _datasetKey);
        Directory.CreateDirectory(_parquetDir);

        _dataSource = AppConfig.Current.DuckDb.UseInMemory ? ":memory:" : "./cnpj.duckdb";
        _connection = new($"Data Source={_dataSource}");
        _connection.Open();

        ConfigureDuckDb(_connection);

        AnsiConsole.MarkupLine("[green]ParquetIngestor inicializado com DuckDB (otimizado)[/]");
    }

    private void ConfigureDuckDb(DuckDBConnection connection)
    {
        try
        {
            Directory.CreateDirectory("./temp");

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                PRAGMA threads = {Math.Max(AppConfig.Current.DuckDb.ThreadsPragma, 1)};
                SET memory_limit = '{AppConfig.Current.DuckDb.MemoryLimit}';
                SET threads = {Math.Max(AppConfig.Current.DuckDb.EngineThreads, 1)};
                SET partitioned_write_max_open_files = {Math.Max(AppConfig.Current.DuckDb.PartitionedWriteMaxOpenFiles, 1)};
                
                PRAGMA temp_directory='./temp';
                PRAGMA enable_progress_bar=false;
                PRAGMA enable_object_cache=true;
                SET preserve_insertion_order={(AppConfig.Current.DuckDb.PreserveInsertionOrder ? "true" : "false")};
            ";
            cmd.ExecuteNonQuery();

            AnsiConsole.MarkupLine("[green]✓ Configurações de performance aplicadas[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[yellow]Aviso ao configurar performance: {ex.Message.EscapeMarkup()}[/]");
        }
    }

    public string? DatasetKey => _datasetKey;

    public async Task ConvertCsvsToParquet()
    {
        var tableConfigs = new Dictionary<string, (string Pattern, string[] Columns)>
        {
            ["empresa"] = ("*EMPRECSV*", [
                "cnpj_basico", "razao_social", "natureza_juridica",
                "qualificacao_responsavel", "capital_social", "porte_empresa", "ente_federativo"
            ]),
            ["estabelecimento"] = ("*ESTABELE*", [
                "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
                "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
                "motivo_situacao_cadastral", "nome_cidade_exterior", "codigo_pais",
                "data_inicio_atividade", "cnae_principal", "cnaes_secundarios",
                "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
                "cep", "uf", "codigo_municipio", "ddd1", "telefone1", "ddd2",
                "telefone2", "ddd_fax", "fax", "correio_eletronico", "situacao_especial",
                "data_situacao_especial"
            ]),
            ["socio"] = ("*SOCIOCSV*", [
                "cnpj_basico", "identificador_socio", "nome_socio", "cnpj_cpf_socio",
                "qualificacao_socio", "data_entrada_sociedade", "codigo_pais",
                "representante_legal", "nome_representante", "qualificacao_representante",
                "faixa_etaria"
            ]),
            ["simples"] = ("*SIMPLES*", [
                "cnpj_basico", "opcao_simples", "data_opcao_simples",
                "data_exclusao_simples", "opcao_mei", "data_opcao_mei",
                "data_exclusao_mei"
            ]),
            ["cnae"] = ("*CNAECSV*", ["codigo", "descricao"]),
            ["motivo"] = ("*MOTICSV*", ["codigo", "descricao"]),
            ["municipio"] = ("*MUNICCSV*", ["codigo", "descricao"]),
            ["natureza"] = ("*NATJUCSV*", ["codigo", "descricao"]),
            ["pais"] = ("*PAISCSV*", ["codigo", "descricao"]),
            ["qualificacao"] = ("*QUALSCSV*", ["codigo", "descricao"])
        };

        await AnsiConsole.Progress()
            .StartAsync(async ctx =>
            {
                foreach (var (tableName, (pattern, columns)) in tableConfigs)
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
                    await ConvertTableToParquet(tableName, files, columns, task);
                }
            });

    }

    private async Task ConvertTableToParquet(string tableName, string[] csvFiles, string[] columns, ProgressTask task)
    {
        var parquetPath = Path.Combine(_parquetDir, $"{tableName}.parquet");
        var partitionedDir = Path.Combine(_parquetDir, tableName);

        string[] shardedSourceTables = ["estabelecimento", "empresa", "simples", "socio"];

        if (shardedSourceTables.Contains(tableName))
        {
            await DirectoryUtils.RecreateDirectoryAsync(partitionedDir);

            for (var index = 0; index < csvFiles.Length; index++)
            {
                var csvFile = csvFiles[index];
                var sourceSql = BuildCsvSourceRelationSql([csvFile], columns);
                var exportSql = $@"
                    COPY (
                        SELECT *,
                               SUBSTRING(cnpj_basico, 1, {AppConfig.Current.Shards.PrefixLength}) as cnpj_prefix
                        FROM {sourceSql} AS src
                    )
                    TO '{EscapeSqlLiteral(partitionedDir)}'
                    (
                        FORMAT PARQUET,
                        COMPRESSION ZSTD,
                        PARTITION_BY (cnpj_prefix),
                        APPEND,
                        FILENAME_PATTERN 'chunk_{index:D3}_{{uuid}}'
                    )";

                await using var exportCmd = _connection.CreateCommand();
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
                TO '{EscapeSqlLiteral(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";

            await using var exportCmd = _connection.CreateCommand();
            exportCmd.CommandText = exportSql;
            await exportCmd.ExecuteNonQueryAsync();

            task.Value = task.MaxValue;
            AnsiConsole.MarkupLine($"[green]✓ {tableName}.parquet criado[/]");
        }
    }

    public async Task ExportAndUploadToStorage(string outputRootDir = "cnpj_shards")
    {
        var outputDir = DatasetPathResolver.GetDatasetPath(outputRootDir, _datasetKey);
        Directory.CreateDirectory(outputDir);

        AnsiConsole.MarkupLine("[cyan]Carregando tabelas auxiliares para geração de shards...[/]");
        await LoadParquetTablesForConnection(_connection, includeShardTables: false);

        AnsiConsole.MarkupLine("[cyan]🚀 Iniciando geração e upload de shards...[/]");

        await ExportShardsToStorage(outputDir);

        AnsiConsole.MarkupLine("[green]🎉 Geração e upload de shards concluídos![/]");
    }

    private async Task LoadParquetTablesForConnection(DuckDBConnection connection, bool includeShardTables = true)
    {
        var tableConfigs = new Dictionary<string, string>();

        if (includeShardTables)
        {
            tableConfigs["empresa"] = "empresa/**/*.parquet";
            tableConfigs["estabelecimento"] = "estabelecimento/**/*.parquet";
            tableConfigs["socio"] = "socio/**/*.parquet";
            tableConfigs["simples"] = "simples/**/*.parquet";
        }

        tableConfigs["cnae"] = "cnae.parquet";
        tableConfigs["motivo"] = "motivo.parquet";
        tableConfigs["municipio"] = "municipio.parquet";
        tableConfigs["natureza"] = "natureza.parquet";
        tableConfigs["pais"] = "pais.parquet";
        tableConfigs["qualificacao"] = "qualificacao.parquet";

        foreach (var (tableName, pattern) in tableConfigs)
        {
            try
            {
                var fullPath = Path.Combine(_parquetDir, pattern);
                var createViewSql = IsShardPartitionedTable(tableName)
                    ? $"CREATE OR REPLACE VIEW {tableName} AS SELECT * FROM read_parquet('{fullPath}', hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})"
                    : $"CREATE OR REPLACE VIEW {tableName} AS SELECT * FROM read_parquet('{fullPath}')";

                await using var cmd = connection.CreateCommand();
                cmd.CommandText = createViewSql;
                await cmd.ExecuteNonQueryAsync();

                if (connection == _connection)
                {
                    AnsiConsole.MarkupLine($"[green]✓ Tabela {tableName} carregada[/]");
                }
            }
            catch (Exception ex)
            {
                if (connection == _connection)
                {
                    AnsiConsole.MarkupLine($"[yellow]Aviso ao carregar {tableName}: {ex.Message.EscapeMarkup()}[/]");
                }
            }
        }
    }

    private async Task ExportShardsToStorage(string outputDir)
    {
        var localShardDir = Path.Combine(outputDir, AppConfig.Current.Shards.RemoteDir);
        await DirectoryUtils.RecreateDirectoryAsync(localShardDir);

        var prefixesToProcess = GetExistingShardPrefixesFromFilesystem();
        var emptyCount = 0;
        var batchSize = Math.Max(1, AppConfig.Current.Shards.QueryBatchSize);
        var prefixBatches = BuildShardPrefixBatches(prefixesToProcess, batchSize);
        var workerBudget = Math.Max(1, AppConfig.Current.Shards.MaxParallelProcessing);
        var workerCount = Math.Max(1, Math.Min(Math.Max(1, workerBudget / batchSize), prefixBatches.Count));
        var queue = new ConcurrentQueue<List<string>>(prefixBatches);
        var progressLock = new object();

        await AnsiConsole.Progress()
            .StartAsync(async ctx =>
            {
                var generationTask = ctx.AddTask("[green]Gerando shards[/]", maxValue: prefixesToProcess.Count);
                var uploadTask = ctx.AddTask("[blue]Enviando shards[/]", maxValue: 100);
                uploadTask.Value = 0;
                uploadTask.Description = "[grey]Upload aguardando geração dos shards[/]";

                if (prefixesToProcess.Count == 0)
                {
                    generationTask.StopTask();
                    return;
                }

                var workers = Enumerable.Range(0, workerCount)
                    .Select(_ => Task.Run(async () =>
                    {
                        await using var workerConnection = await CreateConfiguredConnectionAsync();
                        await LoadParquetTablesForConnection(workerConnection, includeShardTables: false);

                        while (queue.TryDequeue(out var prefixBatch))
                        {
                            var batchResults = await ExportPrefixBatchAsync(workerConnection, prefixBatch, localShardDir);
                            var batchEmptyCount = prefixBatch.Count(prefix => !batchResults.GetValueOrDefault(prefix));
                            Interlocked.Add(ref emptyCount, batchEmptyCount);

                            lock (progressLock)
                            {
                                generationTask.Description =
                                    $"[cyan]Lote {prefixBatch.First()}..{prefixBatch.Last()} gerado[/]";
                                generationTask.Increment(prefixBatch.Count);
                            }
                        }
                    }))
                    .ToArray();

                await Task.WhenAll(workers);

                uploadTask.Description = $"[blue]Enviando diretório {AppConfig.Current.Shards.RemoteDir}[/]";
                var uploaded = await RcloneClient.UploadFolderAsync(
                    localShardDir,
                    AppConfig.Current.Shards.RemoteDir,
                    uploadTask);

                if (!uploaded)
                    throw new InvalidOperationException($"Falha ao enviar diretório {AppConfig.Current.Shards.RemoteDir}.");

                uploadTask.Value = uploadTask.MaxValue;
            });

        var uploadedCount = prefixesToProcess.Count - emptyCount;
        AnsiConsole.MarkupLine(
            $"[green]✓ Shards processados[/] [grey](enviados: {uploadedCount}, vazios: {emptyCount}, workers: {workerCount}, batch: {batchSize})[/]");
    }

    private async Task<bool> ExportSinglePrefixShardAsync(DuckDBConnection connection, string prefixStr, string outputDir)
    {
        Directory.CreateDirectory(outputDir);

        var finalDataPath = Path.Combine(outputDir, $"{prefixStr}{ShardDataExtension}");
        var finalIndexPath = Path.Combine(outputDir, $"{prefixStr}{ShardIndexExtension}");
        var tempDataPath = finalDataPath + ".tmp";
        var tempIndexPath = finalIndexPath + ".tmp";

        var rowCount = await WriteShardNdjsonAsync(
            connection,
            prefixStr,
            tempDataPath,
            tempIndexPath,
            $"{prefixStr}{ShardDataExtension}");

        if (rowCount == 0)
        {
            DeleteIfExists(tempDataPath);
            DeleteIfExists(tempIndexPath);
            return false;
        }

        ReplaceFile(tempDataPath, finalDataPath);
        ReplaceFile(tempIndexPath, finalIndexPath);
        return true;
    }

    private async Task<Dictionary<string, bool>> ExportPrefixBatchAsync(
        DuckDBConnection connection,
        IReadOnlyList<string> prefixes,
        string outputDir)
    {
        var availablePrefixes = prefixes
            .Where(prefix => PartitionHasParquetFiles("estabelecimento", prefix))
            .ToList();
        var counts = prefixes.ToDictionary(prefix => prefix, _ => 0, StringComparer.Ordinal);

        if (availablePrefixes.Count == 0)
            return counts.ToDictionary(kvp => kvp.Key, _ => false, StringComparer.Ordinal);

        var tempDataFiles = prefixes.ToDictionary(
            prefix => prefix,
            prefix => Path.Combine(outputDir, $"{prefix}{ShardDataExtension}.tmp"),
            StringComparer.Ordinal);
        var finalDataFiles = prefixes.ToDictionary(
            prefix => prefix,
            prefix => Path.Combine(outputDir, $"{prefix}{ShardDataExtension}"),
            StringComparer.Ordinal);
        var tempIndexFiles = prefixes.ToDictionary(
            prefix => prefix,
            prefix => Path.Combine(outputDir, $"{prefix}{ShardIndexExtension}.tmp"),
            StringComparer.Ordinal);
        var finalIndexFiles = prefixes.ToDictionary(
            prefix => prefix,
            prefix => Path.Combine(outputDir, $"{prefix}{ShardIndexExtension}"),
            StringComparer.Ordinal);
        var writers = new Dictionary<string, SparseShardWriter>(StringComparer.Ordinal);

        try
        {
            foreach (var prefix in availablePrefixes)
            {
                var writer = new SparseShardWriter(
                    prefix,
                    tempDataFiles[prefix],
                    AppConfig.Current.Shards.SparseIndexStride);
                writers[prefix] = writer;
            }

            var query = BuildJsonQueryForPrefixBatch(availablePrefixes, includeCnpjColumn: true, jsonAlias: "json_data");
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = query;

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var prefix = reader.GetString(0);
                var cnpj = reader.GetString(1);
                var jsonData = reader.GetString(2);

                var writer = writers[prefix];
                await writer.AppendAsync(cnpj, jsonData);
                counts[prefix]++;
            }

            foreach (var prefix in availablePrefixes)
            {
                var writer = writers[prefix];
                await writer.FlushAsync();
            }
        }
        finally
        {
            foreach (var writer in writers.Values)
                writer.Dispose();
        }

        foreach (var prefix in availablePrefixes)
        {
            if (counts[prefix] == 0)
            {
                DeleteIfExists(tempDataFiles[prefix]);
                DeleteIfExists(tempIndexFiles[prefix]);
                continue;
            }

            var indexDocument = writers[prefix].BuildIndexDocument($"{prefix}{ShardDataExtension}");
            await WriteJsonFileAsync(tempIndexFiles[prefix], indexDocument);

            ReplaceFile(tempDataFiles[prefix], finalDataFiles[prefix]);
            ReplaceFile(tempIndexFiles[prefix], finalIndexFiles[prefix]);
        }

        return counts.ToDictionary(kvp => kvp.Key, kvp => kvp.Value > 0, StringComparer.Ordinal);
    }

    /// <summary>
    /// Exporta um CNPJ específico para arquivo JSON individual
    /// </summary>
    /// <param name="cnpj">CNPJ com 14 caracteres normalizados (alfanumérico suportado)</param>
    /// <param name="outputDir">Diretório de saída</param>
    public async ValueTask ExportSingleCnpjAsync(string cnpj, string outputDir)
    {
        var resolvedOutputDir = DatasetPathResolver.GetDatasetPath(outputDir, _datasetKey);
        Directory.CreateDirectory(resolvedOutputDir);

        await ConvertCsvsToParquet();

        AnsiConsole.MarkupLine("[cyan]Carregando tabelas auxiliares para exportação individual...[/]");
        await LoadParquetTablesForConnection(_connection, includeShardTables: false);

        var normalizedCnpj = CnpjUtils.RemoveMask(cnpj);
        var (cnpjBasico, cnpjOrdem, cnpjDv) = CnpjUtils.ParseCnpj(normalizedCnpj);
        var prefixStr = normalizedCnpj[..AppConfig.Current.Shards.PrefixLength];

        AnsiConsole.MarkupLine($"[yellow]🎯 Buscando CNPJ {normalizedCnpj} (prefixo {prefixStr})...[/]");

        try
        {
            var outputFile = Path.Combine(resolvedOutputDir, $"{normalizedCnpj}.json");
            var exportQuery = BuildJsonQueryForCnpj(prefixStr, cnpjBasico, cnpjOrdem, cnpjDv, jsonAlias: "json_output");

            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = exportQuery;

            var result = await cmd.ExecuteScalarAsync();

            if (result != null && result.ToString() != "")
            {
                await File.WriteAllTextAsync(outputFile, result.ToString()!);

                var fileInfo = new FileInfo(outputFile);
                AnsiConsole.MarkupLine($"[green]✓ {cnpj}.json criado ({fileInfo.Length} bytes)[/]");
            }
            else
            {
                AnsiConsole.MarkupLine($"[red]❌ CNPJ {cnpj} não encontrado na base de dados[/]");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Erro exportando CNPJ {cnpj}: {ex.Message.EscapeMarkup()}[/]");
        }
    }

    public async Task ExportSingleShardAsync(string prefix, string outputRootDir)
    {
        var normalizedPrefix = prefix.Trim();
        var resolvedOutputDir = DatasetPathResolver.GetDatasetPath(outputRootDir, _datasetKey);
        var shardDir = Path.Combine(resolvedOutputDir, AppConfig.Current.Shards.RemoteDir);
        Directory.CreateDirectory(shardDir);

        await ConvertCsvsToParquet();

        AnsiConsole.MarkupLine("[cyan]Carregando tabelas auxiliares para exportação do shard...[/]");
        await LoadParquetTablesForConnection(_connection, includeShardTables: false);

        var exported = await ExportSinglePrefixShardAsync(_connection, normalizedPrefix, shardDir);

        if (!exported)
        {
            AnsiConsole.MarkupLine($"[yellow]⚠️ Shard {normalizedPrefix} não possui registros[/]");
            return;
        }

        var dataInfo = new FileInfo(Path.Combine(shardDir, $"{normalizedPrefix}{ShardDataExtension}"));
        var indexInfo = new FileInfo(Path.Combine(shardDir, $"{normalizedPrefix}{ShardIndexExtension}"));
        AnsiConsole.MarkupLine($"[green]✓ Shard {normalizedPrefix}{ShardDataExtension} criado ({dataInfo.Length} bytes) + índice ({indexInfo.Length} bytes)[/]");
    }

    /// <summary>
    /// Exporta todos os shards diretamente para ZIP sem criar arquivos temporários em disco
    /// </summary>
    public async Task GenerateAndUploadFinalInfoJsonAsync()
    {
        try
        {
            // Garante que as views estejam disponíveis para contagem
            await LoadParquetTablesForConnection(_connection);

            long total = 0;
            await using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = "SELECT COUNT(*) FROM estabelecimento";
                var result = await cmd.ExecuteScalarAsync();
                if (result != null && long.TryParse(result.ToString(), out var count))
                {
                    total = count;
                }
            }

            var lastUpdated = DateTime.UtcNow.ToString("o");

            var payload = new
            {
                total = total,
                last_updated = lastUpdated,
                zip_available = false,
                zip_size = 0L,
                zip_url = "",
                zip_md5checksum = "",
                shard_prefix_length = AppConfig.Current.Shards.PrefixLength,
                shard_count = GetShardCountFromFilesystem(),
                shard_path_template = $"{AppConfig.Current.Shards.RemoteDir.Trim('/')}/{{prefix}}{ShardDataExtension}",
                shard_index_path_template = $"{AppConfig.Current.Shards.RemoteDir.Trim('/')}/{{prefix}}{ShardIndexExtension}",
                shard_format = "ndjson+sparse-index",
                zip_layout = "disabled",
                cnpj_type = "string"
            };

            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
            {
                PropertyNamingPolicy = null,
                WriteIndented = false
            });

            var outputDir = DatasetPathResolver.GetDatasetPath(AppConfig.Current.Paths.OutputDir, _datasetKey);
            Directory.CreateDirectory(outputDir);
            var localInfoPath = Path.Combine(outputDir, "info.json");
            await File.WriteAllTextAsync(localInfoPath, json, Encoding.UTF8);

            AnsiConsole.MarkupLine("[cyan]📤 Enviando info.json para Storage...[/]");
            var ok = await RcloneClient.UploadFileAsync(localInfoPath, "info.json");
            if (ok)
            {
                AnsiConsole.MarkupLine("[green]✓ info.json enviado para Storage[/]");
            }
            else
            {
                AnsiConsole.MarkupLine("[red]❌ Falha ao enviar info.json para Storage[/]");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Erro ao gerar/enviar info.json: {ex.Message.EscapeMarkup()}[/]");
        }
    }

    private async Task<int> WriteShardNdjsonAsync(
        DuckDBConnection connection,
        string prefixStr,
        string outputPath,
        string indexPath,
        string finalDataFileName)
    {
        var query = BuildJsonQueryForPrefix(prefixStr, includeCnpjColumn: true, jsonAlias: "json_data");

        if (query is null)
            return 0;

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = query;

        await using var reader = await cmd.ExecuteReaderAsync();
        using var writer = new SparseShardWriter(prefixStr, outputPath, AppConfig.Current.Shards.SparseIndexStride);

        while (await reader.ReadAsync())
        {
            await writer.AppendAsync(reader.GetString(0), reader.GetString(1));
        }

        await writer.FlushAsync();

        if (writer.RecordCount == 0)
            return 0;

        await WriteJsonFileAsync(indexPath, writer.BuildIndexDocument(finalDataFileName));
        return writer.RecordCount;
    }

    private static string? ResolveDatasetKey(string? datasetKey)
    {
        if (DatasetPathResolver.IsDatasetKey(datasetKey))
            return datasetKey;

        return DatasetPathResolver.ResolveLatestLocalDatasetKey(AppConfig.Current.Paths);
    }

    private static string BuildCsvSourceRelationSql(IEnumerable<string> csvFiles, IReadOnlyList<string> columns)
    {
        var fileListSql = string.Join(", ", csvFiles.Select(file => $"'{EscapeSqlLiteral(file)}'"));
        var columnsSql = string.Join(", ", columns.Select(column => $"'{EscapeSqlLiteral(column)}': 'VARCHAR'"));

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

    private static string EscapeSqlLiteral(string value)
    {
        return value.Replace("'", "''");
    }

    private static async Task WriteJsonFileAsync<T>(string path, T payload)
    {
        await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        await JsonSerializer.SerializeAsync(stream, payload, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            WriteIndented = false
        });
        await stream.FlushAsync();
    }

    private static void ReplaceFile(string sourcePath, string destinationPath)
    {
        DeleteIfExists(destinationPath);
        File.Move(sourcePath, destinationPath);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static bool IsShardPartitionedTable(string tableName)
    {
        return tableName is "estabelecimento" or "empresa" or "simples" or "socio";
    }

    private async Task<DuckDBConnection> CreateConfiguredConnectionAsync()
    {
        var connection = new DuckDBConnection($"Data Source={_dataSource}");
        await connection.OpenAsync();
        ConfigureDuckDb(connection);
        return connection;
    }

    private static void AddParameter(System.Data.Common.DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    private List<string> GetExistingShardPrefixesFromFilesystem()
    {
        var partitionedDir = Path.Combine(_parquetDir, "estabelecimento");
        if (!Directory.Exists(partitionedDir))
            return [];

        return Directory.EnumerateDirectories(partitionedDir, "cnpj_prefix=*", SearchOption.TopDirectoryOnly)
            .Select(Path.GetFileName)
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Select(static name => name!["cnpj_prefix=".Length..])
            .Where(static prefix => !string.IsNullOrWhiteSpace(prefix))
            .OrderBy(static prefix => prefix, StringComparer.Ordinal)
            .ToList();
    }

    private int GetShardCountFromFilesystem()
    {
        return GetExistingShardPrefixesFromFilesystem().Count;
    }

    private static List<List<string>> BuildShardPrefixBatches(IReadOnlyList<string> prefixes, int batchSize)
    {
        var batches = new List<List<string>>();
        for (var index = 0; index < prefixes.Count; index += batchSize)
        {
            batches.Add(prefixes.Skip(index).Take(batchSize).ToList());
        }

        return batches;
    }

    private static string GetJsonStructFields()
    {
        return @"cnpj := e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv,
                    razao_social := COALESCE(emp.razao_social, ''),
                    nome_fantasia := COALESCE(e.nome_fantasia, ''),
                    situacao_cadastral := CASE LPAD(e.situacao_cadastral, 2, '0')
                        WHEN '01' THEN 'Nula'
                        WHEN '02' THEN 'Ativa'
                        WHEN '03' THEN 'Suspensa'
                        WHEN '04' THEN 'Inapta'
                        WHEN '08' THEN 'Baixada'
                        ELSE e.situacao_cadastral
                    END,
                    data_situacao_cadastral := CASE 
                        WHEN e.data_situacao_cadastral ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(e.data_situacao_cadastral, 1, 4) || '-' || 
                             SUBSTRING(e.data_situacao_cadastral, 5, 2) || '-' || 
                             SUBSTRING(e.data_situacao_cadastral, 7, 2)
                        ELSE COALESCE(e.data_situacao_cadastral, '')
                    END,
                    matriz_filial := CASE e.identificador_matriz_filial
                        WHEN '1' THEN 'Matriz'
                        WHEN '2' THEN 'Filial'
                        ELSE e.identificador_matriz_filial
                    END,
                    data_inicio_atividade := CASE 
                        WHEN e.data_inicio_atividade ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(e.data_inicio_atividade, 1, 4) || '-' || 
                             SUBSTRING(e.data_inicio_atividade, 5, 2) || '-' || 
                             SUBSTRING(e.data_inicio_atividade, 7, 2)
                        ELSE COALESCE(e.data_inicio_atividade, '')
                    END,
                    cnae_principal := COALESCE(e.cnae_principal, ''),
                    cnaes_secundarios := CASE 
                        WHEN e.cnaes_secundarios IS NOT NULL AND e.cnaes_secundarios != ''
                        THEN string_split(e.cnaes_secundarios, ',')
                        ELSE []
                    END,
                    natureza_juridica := COALESCE(nat.descricao, ''),
                    tipo_logradouro := COALESCE(e.tipo_logradouro, ''),
                    logradouro := COALESCE(e.logradouro, ''),
                    numero := COALESCE(e.numero, ''),
                    complemento := COALESCE(e.complemento, ''),
                    bairro := COALESCE(e.bairro, ''),
                    cep := COALESCE(e.cep, ''),
                    uf := COALESCE(e.uf, ''),
                    municipio := COALESCE(mun.descricao, ''),
                    email := COALESCE(e.correio_eletronico, ''),
                    telefones := list_filter([
                        CASE WHEN e.ddd1 IS NOT NULL OR e.telefone1 IS NOT NULL
                             THEN struct_pack(ddd := COALESCE(e.ddd1, ''), numero := COALESCE(e.telefone1, ''), is_fax := false)
                             ELSE NULL
                        END,
                        CASE WHEN e.ddd2 IS NOT NULL OR e.telefone2 IS NOT NULL  
                             THEN struct_pack(ddd := COALESCE(e.ddd2, ''), numero := COALESCE(e.telefone2, ''), is_fax := false)
                             ELSE NULL
                        END,
                        CASE WHEN e.ddd_fax IS NOT NULL OR e.fax IS NOT NULL
                             THEN struct_pack(ddd := COALESCE(e.ddd_fax, ''), numero := COALESCE(e.fax, ''), is_fax := true)
                             ELSE NULL
                        END
                    ], x -> x IS NOT NULL),
                    capital_social := COALESCE(emp.capital_social, ''),
                    porte_empresa := CASE emp.porte_empresa
                        WHEN '00' THEN 'Não informado'
                        WHEN '01' THEN 'Microempresa (ME)'
                        WHEN '03' THEN 'Empresa de Pequeno Porte (EPP)'
                        WHEN '05' THEN 'Demais'
                        ELSE COALESCE(emp.porte_empresa, '')
                    END,
                    opcao_simples := COALESCE(s.opcao_simples, ''),
                    data_opcao_simples := CASE 
                        WHEN s.data_opcao_simples ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(s.data_opcao_simples, 1, 4) || '-' || 
                             SUBSTRING(s.data_opcao_simples, 5, 2) || '-' || 
                             SUBSTRING(s.data_opcao_simples, 7, 2)
                        ELSE COALESCE(s.data_opcao_simples, '')
                    END,
                    opcao_mei := COALESCE(s.opcao_mei, ''),
                    data_opcao_mei := CASE 
                        WHEN s.data_opcao_mei ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(s.data_opcao_mei, 1, 4) || '-' || 
                             SUBSTRING(s.data_opcao_mei, 5, 2) || '-' || 
                             SUBSTRING(s.data_opcao_mei, 7, 2)
                        ELSE COALESCE(s.data_opcao_mei, '')
                    END,
                    QSA := COALESCE(sd.qsa_data, [])";
    }

    private bool PartitionHasParquetFiles(string tableName, string prefix)
    {
        var partitionDir = Path.Combine(_parquetDir, tableName, $"cnpj_prefix={prefix}");
        return Directory.Exists(partitionDir)
               && Directory.EnumerateFiles(partitionDir, "*.parquet", SearchOption.TopDirectoryOnly).Any();
    }

    private IEnumerable<string> GetPartitionGlobPaths(string tableName, IReadOnlyList<string> prefixes)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var prefix in prefixes)
        {
            var partitionDir = Path.Combine(_parquetDir, tableName, $"cnpj_prefix={prefix}");
            if (!Directory.Exists(partitionDir))
                continue;

            var glob = Path.Combine(partitionDir, "*.parquet");
            if (seen.Add(glob))
                yield return glob;
        }
    }

    private string BuildPartitionedReadSql(string tableName, IReadOnlyList<string> prefixes, bool allowEmpty)
    {
        var globs = GetPartitionGlobPaths(tableName, prefixes).ToList();
        if (globs.Count == 0)
        {
            if (!allowEmpty)
                throw new InvalidOperationException($"Nenhuma partição Parquet encontrada para {tableName}.");

            return BuildEmptyShardTableSql(tableName);
        }

        var pathListSql = string.Join(", ", globs.Select(path => $"'{EscapeSqlLiteral(path)}'"));
        return $"read_parquet([{pathListSql}], hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})";
    }

    private static string BuildEmptyShardTableSql(string tableName)
    {
        return tableName switch
        {
            "empresa" =>
                "(SELECT CAST(NULL AS VARCHAR) AS cnpj_basico, CAST(NULL AS VARCHAR) AS razao_social, CAST(NULL AS VARCHAR) AS natureza_juridica, CAST(NULL AS VARCHAR) AS qualificacao_responsavel, CAST(NULL AS VARCHAR) AS capital_social, CAST(NULL AS VARCHAR) AS porte_empresa, CAST(NULL AS VARCHAR) AS ente_federativo, CAST(NULL AS VARCHAR) AS cnpj_prefix WHERE FALSE)",
            "simples" =>
                "(SELECT CAST(NULL AS VARCHAR) AS cnpj_basico, CAST(NULL AS VARCHAR) AS opcao_simples, CAST(NULL AS VARCHAR) AS data_opcao_simples, CAST(NULL AS VARCHAR) AS data_exclusao_simples, CAST(NULL AS VARCHAR) AS opcao_mei, CAST(NULL AS VARCHAR) AS data_opcao_mei, CAST(NULL AS VARCHAR) AS data_exclusao_mei, CAST(NULL AS VARCHAR) AS cnpj_prefix WHERE FALSE)",
            "socio" =>
                "(SELECT CAST(NULL AS VARCHAR) AS cnpj_basico, CAST(NULL AS VARCHAR) AS identificador_socio, CAST(NULL AS VARCHAR) AS nome_socio, CAST(NULL AS VARCHAR) AS cnpj_cpf_socio, CAST(NULL AS VARCHAR) AS qualificacao_socio, CAST(NULL AS VARCHAR) AS data_entrada_sociedade, CAST(NULL AS VARCHAR) AS codigo_pais, CAST(NULL AS VARCHAR) AS representante_legal, CAST(NULL AS VARCHAR) AS nome_representante, CAST(NULL AS VARCHAR) AS qualificacao_representante, CAST(NULL AS VARCHAR) AS faixa_etaria, CAST(NULL AS VARCHAR) AS cnpj_prefix WHERE FALSE)",
            "estabelecimento" =>
                "(SELECT CAST(NULL AS VARCHAR) AS cnpj_basico, CAST(NULL AS VARCHAR) AS cnpj_ordem, CAST(NULL AS VARCHAR) AS cnpj_dv, CAST(NULL AS VARCHAR) AS identificador_matriz_filial, CAST(NULL AS VARCHAR) AS nome_fantasia, CAST(NULL AS VARCHAR) AS situacao_cadastral, CAST(NULL AS VARCHAR) AS data_situacao_cadastral, CAST(NULL AS VARCHAR) AS motivo_situacao_cadastral, CAST(NULL AS VARCHAR) AS nome_cidade_exterior, CAST(NULL AS VARCHAR) AS codigo_pais, CAST(NULL AS VARCHAR) AS data_inicio_atividade, CAST(NULL AS VARCHAR) AS cnae_principal, CAST(NULL AS VARCHAR) AS cnaes_secundarios, CAST(NULL AS VARCHAR) AS tipo_logradouro, CAST(NULL AS VARCHAR) AS logradouro, CAST(NULL AS VARCHAR) AS numero, CAST(NULL AS VARCHAR) AS complemento, CAST(NULL AS VARCHAR) AS bairro, CAST(NULL AS VARCHAR) AS cep, CAST(NULL AS VARCHAR) AS uf, CAST(NULL AS VARCHAR) AS codigo_municipio, CAST(NULL AS VARCHAR) AS ddd1, CAST(NULL AS VARCHAR) AS telefone1, CAST(NULL AS VARCHAR) AS ddd2, CAST(NULL AS VARCHAR) AS telefone2, CAST(NULL AS VARCHAR) AS ddd_fax, CAST(NULL AS VARCHAR) AS fax, CAST(NULL AS VARCHAR) AS correio_eletronico, CAST(NULL AS VARCHAR) AS situacao_especial, CAST(NULL AS VARCHAR) AS data_situacao_especial, CAST(NULL AS VARCHAR) AS cnpj_prefix WHERE FALSE)",
            _ => throw new InvalidOperationException($"Tabela shard não suportada: {tableName}")
        };
    }

    private string? BuildJsonQueryForPrefix(string prefixStr, bool includeCnpjColumn, string jsonAlias)
    {
        if (!PartitionHasParquetFiles("estabelecimento", prefixStr))
            return null;

        var jsonFields = GetJsonStructFields();
        var prefixLiteral = EscapeSqlLiteral(prefixStr);
        var prefixes = new[] { prefixStr };
        var estabelecimentoRelation = BuildPartitionedReadSql("estabelecimento", prefixes, allowEmpty: false);
        var empresaRelation = BuildPartitionedReadSql("empresa", prefixes, allowEmpty: false);
        var simplesRelation = BuildPartitionedReadSql("simples", prefixes, allowEmpty: true);
        var socioRelation = BuildPartitionedReadSql("socio", prefixes, allowEmpty: true);
        var selectCols = includeCnpjColumn
            ? "e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj, to_json(struct_pack(\n" + jsonFields +
              $"\n)) as {jsonAlias}"
            : $"to_json(struct_pack(\n" + jsonFields + $"\n)) as {jsonAlias}";

        return $@"WITH estabelecimento_data AS (
                SELECT * FROM {estabelecimentoRelation}
            ),
            empresa_data AS (
                SELECT * FROM {empresaRelation}
            ),
            simples_data AS (
                SELECT * FROM {simplesRelation}
            ),
            socio_data AS (
                SELECT * FROM {socioRelation}
            ),
            socios_data AS (
                SELECT 
                    s.cnpj_prefix,
                    s.cnpj_basico,
                    array_agg(struct_pack(
                        nome_socio := COALESCE(s.nome_socio, ''),
                        cnpj_cpf_socio := COALESCE(s.cnpj_cpf_socio, ''),
                        qualificacao_socio := COALESCE(qs.descricao, ''),
                        data_entrada_sociedade := CASE 
                            WHEN s.data_entrada_sociedade ~ '^[0-9]{{8}}$' 
                            THEN SUBSTRING(s.data_entrada_sociedade, 1, 4) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 5, 2) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 7, 2)
                            ELSE COALESCE(s.data_entrada_sociedade, '')
                        END,
                        identificador_socio := CASE s.identificador_socio
                            WHEN '1' THEN 'Pessoa Jurídica'
                            WHEN '2' THEN 'Pessoa Física'
                            WHEN '3' THEN 'Estrangeiro'
                            ELSE COALESCE(s.identificador_socio, '')
                        END,
                        faixa_etaria := CASE s.faixa_etaria
                            WHEN '0' THEN 'Não se aplica'
                            WHEN '1' THEN '0 a 12 anos'
                            WHEN '2' THEN '13 a 20 anos'
                            WHEN '3' THEN '21 a 30 anos'
                            WHEN '4' THEN '31 a 40 anos'
                            WHEN '5' THEN '41 a 50 anos'
                            WHEN '6' THEN '51 a 60 anos'
                            WHEN '7' THEN '61 a 70 anos'
                            WHEN '8' THEN '71 a 80 anos'
                            WHEN '9' THEN 'Mais de 80 anos'
                            ELSE COALESCE(s.faixa_etaria, '')
                        END
                    )) as qsa_data
                FROM socio_data s
                LEFT JOIN qualificacao qs ON s.qualificacao_socio = qs.codigo
                WHERE s.cnpj_prefix = '{prefixLiteral}'
                GROUP BY s.cnpj_prefix, s.cnpj_basico
            )
            SELECT {selectCols}
            FROM estabelecimento_data e
            LEFT JOIN empresa_data emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN simples_data s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN socios_data sd ON e.cnpj_prefix = sd.cnpj_prefix AND e.cnpj_basico = sd.cnpj_basico
            WHERE e.cnpj_prefix = '{prefixLiteral}'";
    }

    private string BuildJsonQueryForPrefixBatch(IReadOnlyList<string> prefixes, bool includeCnpjColumn, string jsonAlias)
    {
        var jsonFields = GetJsonStructFields();
        var estabelecimentoRelation = BuildPartitionedReadSql("estabelecimento", prefixes, allowEmpty: false);
        var empresaRelation = BuildPartitionedReadSql("empresa", prefixes, allowEmpty: false);
        var simplesRelation = BuildPartitionedReadSql("simples", prefixes, allowEmpty: true);
        var socioRelation = BuildPartitionedReadSql("socio", prefixes, allowEmpty: true);
        var selectCols = includeCnpjColumn
            ? "e.cnpj_prefix as shard_prefix, e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj, to_json(struct_pack(\n" +
              jsonFields + $"\n)) as {jsonAlias}"
            : $"e.cnpj_prefix as shard_prefix, to_json(struct_pack(\n{jsonFields}\n)) as {jsonAlias}";

        return $@"WITH batch_estabelecimentos AS (
                SELECT * FROM {estabelecimentoRelation}
            ),
            batch_empresas AS (
                SELECT * FROM {empresaRelation}
            ),
            batch_simples AS (
                SELECT * FROM {simplesRelation}
            ),
            batch_socios AS (
                SELECT
                    s.cnpj_prefix,
                    s.cnpj_basico,
                    array_agg(struct_pack(
                        nome_socio := COALESCE(s.nome_socio, ''),
                        cnpj_cpf_socio := COALESCE(s.cnpj_cpf_socio, ''),
                        qualificacao_socio := COALESCE(qs.descricao, ''),
                        data_entrada_sociedade := CASE
                            WHEN s.data_entrada_sociedade ~ '^[0-9]{{8}}$'
                            THEN SUBSTRING(s.data_entrada_sociedade, 1, 4) || '-' ||
                                 SUBSTRING(s.data_entrada_sociedade, 5, 2) || '-' ||
                                 SUBSTRING(s.data_entrada_sociedade, 7, 2)
                            ELSE COALESCE(s.data_entrada_sociedade, '')
                        END,
                        identificador_socio := CASE s.identificador_socio
                            WHEN '1' THEN 'Pessoa Jurídica'
                            WHEN '2' THEN 'Pessoa Física'
                            WHEN '3' THEN 'Estrangeiro'
                            ELSE COALESCE(s.identificador_socio, '')
                        END,
                        faixa_etaria := CASE s.faixa_etaria
                            WHEN '0' THEN 'Não se aplica'
                            WHEN '1' THEN '0 a 12 anos'
                            WHEN '2' THEN '13 a 20 anos'
                            WHEN '3' THEN '21 a 30 anos'
                            WHEN '4' THEN '31 a 40 anos'
                            WHEN '5' THEN '41 a 50 anos'
                            WHEN '6' THEN '51 a 60 anos'
                            WHEN '7' THEN '61 a 70 anos'
                            WHEN '8' THEN '71 a 80 anos'
                            WHEN '9' THEN 'Mais de 80 anos'
                            ELSE COALESCE(s.faixa_etaria, '')
                        END
                    )) AS qsa_data
                FROM {socioRelation} s
                LEFT JOIN qualificacao qs ON s.qualificacao_socio = qs.codigo
                GROUP BY s.cnpj_prefix, s.cnpj_basico
            )
            SELECT {selectCols}
            FROM batch_estabelecimentos e
            LEFT JOIN batch_empresas emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN batch_simples s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN batch_socios sd ON e.cnpj_prefix = sd.cnpj_prefix AND e.cnpj_basico = sd.cnpj_basico
            ORDER BY e.cnpj_prefix, cnpj";
    }

    private string BuildJsonQueryForCnpj(string prefixStr, string cnpjBasico, string cnpjOrdem, string cnpjDv, string jsonAlias)
    {
        var jsonFields = GetJsonStructFields();
        var prefixLiteral = EscapeSqlLiteral(prefixStr);
        var cnpjBasicoLiteral = EscapeSqlLiteral(cnpjBasico);
        var cnpjOrdemLiteral = EscapeSqlLiteral(cnpjOrdem);
        var cnpjDvLiteral = EscapeSqlLiteral(cnpjDv);
        var prefixes = new[] { prefixStr };
        var estabelecimentoRelation = BuildPartitionedReadSql("estabelecimento", prefixes, allowEmpty: false);
        var empresaRelation = BuildPartitionedReadSql("empresa", prefixes, allowEmpty: false);
        var simplesRelation = BuildPartitionedReadSql("simples", prefixes, allowEmpty: true);
        var socioRelation = BuildPartitionedReadSql("socio", prefixes, allowEmpty: true);
        var selectCols = $"to_json(struct_pack(\n" + jsonFields + $"\n)) as {jsonAlias}";

        return $@"WITH estabelecimento_data AS (
                SELECT * FROM {estabelecimentoRelation}
            ),
            empresa_data AS (
                SELECT * FROM {empresaRelation}
            ),
            simples_data AS (
                SELECT * FROM {simplesRelation}
            ),
            socio_data AS (
                SELECT * FROM {socioRelation}
            ),
            socios_data AS (
                SELECT 
                    s.cnpj_prefix,
                    s.cnpj_basico,
                    array_agg(struct_pack(
                        nome_socio := COALESCE(s.nome_socio, ''),
                        cnpj_cpf_socio := COALESCE(s.cnpj_cpf_socio, ''),
                        qualificacao_socio := COALESCE(qs.descricao, ''),
                        data_entrada_sociedade := CASE 
                            WHEN s.data_entrada_sociedade ~ '^[0-9]{{8}}$' 
                            THEN SUBSTRING(s.data_entrada_sociedade, 1, 4) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 5, 2) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 7, 2)
                            ELSE COALESCE(s.data_entrada_sociedade, '')
                        END,
                        identificador_socio := CASE s.identificador_socio
                            WHEN '1' THEN 'Pessoa Jurídica'
                            WHEN '2' THEN 'Pessoa Física'
                            WHEN '3' THEN 'Estrangeiro'
                            ELSE COALESCE(s.identificador_socio, '')
                        END,
                        faixa_etaria := CASE s.faixa_etaria
                            WHEN '0' THEN 'Não se aplica'
                            WHEN '1' THEN '0 a 12 anos'
                            WHEN '2' THEN '13 a 20 anos'
                            WHEN '3' THEN '21 a 30 anos'
                            WHEN '4' THEN '31 a 40 anos'
                            WHEN '5' THEN '41 a 50 anos'
                            WHEN '6' THEN '51 a 60 anos'
                            WHEN '7' THEN '61 a 70 anos'
                            WHEN '8' THEN '71 a 80 anos'
                            WHEN '9' THEN 'Mais de 80 anos'
                            ELSE COALESCE(s.faixa_etaria, '')
                        END
                    )) as qsa_data
                FROM socio_data s
                LEFT JOIN qualificacao qs ON s.qualificacao_socio = qs.codigo
                WHERE s.cnpj_prefix = '{prefixLiteral}'
                GROUP BY s.cnpj_prefix, s.cnpj_basico
            )
            SELECT {selectCols}
            FROM estabelecimento_data e
            LEFT JOIN empresa_data emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN simples_data s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN socios_data sd ON e.cnpj_prefix = sd.cnpj_prefix AND e.cnpj_basico = sd.cnpj_basico
            WHERE e.cnpj_prefix = '{prefixLiteral}'
              AND e.cnpj_basico = '{cnpjBasicoLiteral}'
              AND e.cnpj_ordem = '{cnpjOrdemLiteral}'
              AND e.cnpj_dv = '{cnpjDvLiteral}'";
    }

    public void Dispose()
    {
        _connection?.Dispose();
    }
}
