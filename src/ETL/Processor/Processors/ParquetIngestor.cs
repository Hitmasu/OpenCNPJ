using System.Collections.Concurrent;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using CNPJExporter.Configuration;
using CNPJExporter.Exporters;
using CNPJExporter.Integrations;
using CNPJExporter.Modules.Receita.Processors;
using CNPJExporter.Processors.Models;
using CNPJExporter.Utils;
using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Processors;

public class ParquetIngestor : IDisposable
{
    private const string ShardDataExtension = ".ndjson";
    private const string ShardIndexExtension = ".index.bin";
    private static readonly UTF8Encoding Utf8NoBom = new(false);
    private readonly string? _datasetKey;
    private readonly string _dataSource;
    private readonly DuckDBConnection _connection;
    private readonly ParquetProcessor _parquetProcessor;
    private readonly ShardQueryBuilder _shardQueryBuilder;

    public ParquetIngestor(string? datasetKey = null)
    {
        _datasetKey = ResolveDatasetKey(datasetKey);
        var dataDir = DatasetPathResolver.GetDatasetPath(AppConfig.Current.Paths.DataDir, _datasetKey);
        var parquetDir = DatasetPathResolver.GetDatasetPath(AppConfig.Current.Paths.ParquetDir, _datasetKey);
        Directory.CreateDirectory(parquetDir);
        _parquetProcessor = new ParquetProcessor(
            dataDir,
            parquetDir,
            AppConfig.Current.Shards.PrefixLength);
        _shardQueryBuilder = new ShardQueryBuilder(parquetDir);

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
        await _parquetProcessor.ConvertCsvsToParquetAsync(_connection);
    }

    public async Task ExportAndUploadToStorage(
        string outputRootDir,
        string releaseId)
    {
        var outputDir = GetDatasetOutputDir(outputRootDir);
        var releaseOutputDir = GetLocalReleaseOutputDir(outputRootDir, releaseId);
        Directory.CreateDirectory(outputDir);
        Directory.CreateDirectory(releaseOutputDir);

        AnsiConsole.MarkupLine("[cyan]Carregando tabelas auxiliares para geração de shards...[/]");
        await LoadParquetTablesForConnection(_connection, includeShardTables: false);

        AnsiConsole.MarkupLine("[cyan]🚀 Iniciando geração e upload de shards...[/]");

        await ExportShardsToStorage(releaseOutputDir, releaseId);

        AnsiConsole.MarkupLine("[green]🎉 Geração e upload de shards concluídos![/]");
    }

    private async Task LoadParquetTablesForConnection(DuckDBConnection connection, bool includeShardTables = true)
    {
        await _parquetProcessor.LoadTablesForConnectionAsync(
            connection,
            includeShardTables,
            showWarnings: connection == _connection);
    }

    private async Task ExportShardsToStorage(
        string outputDir,
        string releaseId)
    {
        var localShardDir = Path.Combine(outputDir, AppConfig.Current.Shards.RemoteDir);
        Directory.CreateDirectory(localShardDir);

        var allPrefixes = _shardQueryBuilder.GetExistingShardPrefixes();
        var releaseRemoteDir = BuildReleaseShardRemoteDir(releaseId);
        var releasePlan = BuildReleasePlan(localShardDir, allPrefixes);
        var emptyCount = 0;
        var batchSize = Math.Max(1, AppConfig.Current.Shards.QueryBatchSize);
        var prefixBatches = BuildShardPrefixBatches(releasePlan.PrefixesToGenerate, batchSize);
        var workerBudget = Math.Max(1, AppConfig.Current.Shards.MaxParallelProcessing);
        var workerCount = Math.Max(1, Math.Min(Math.Max(1, workerBudget / batchSize), prefixBatches.Count));
        var queue = new ConcurrentQueue<List<string>>(prefixBatches);
        var progressLock = new object();

        AnsiConsole.MarkupLine(
            $"[grey]Plano do release:[/] [cyan]reuso local/upload={releasePlan.PrefixesUploadOnly.Count}[/], [cyan]gerar={releasePlan.PrefixesToGenerate.Count}[/]");

        await AnsiConsole.Progress()
            .StartAsync(async ctx =>
            {
                var generationTask = ctx.AddTask("[green]Gerando shards[/]", maxValue: Math.Max(1, releasePlan.PrefixesToGenerate.Count));
                var uploadTask = ctx.AddTask("[blue]Enviando shards[/]", maxValue: 100);
                uploadTask.Value = 0;
                uploadTask.Description = "[grey]Upload aguardando geração dos shards[/]";

                if (releasePlan.PrefixesToGenerate.Count == 0)
                {
                    generationTask.Value = generationTask.MaxValue;
                    generationTask.Description = "[grey]Nenhum shard precisou ser regenerado[/]";
                }
                else
                {
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
                }

                var uploadTargets = BuildUploadTargets(
                    localShardDir,
                    releasePlan.PrefixesUploadOnly.Concat(releasePlan.PrefixesToGenerate));
                uploadTask.Description = uploadTargets.Count == 0
                    ? "[grey]Nenhum shard precisou de upload[/]"
                    : $"[blue]Enviando diff do release {releaseId}[/]";

                var uploaded = await RcloneClient.UploadSelectedFilesAsync(
                    localShardDir,
                    releaseRemoteDir,
                    uploadTargets,
                    uploadTask);

                if (!uploaded)
                    throw new InvalidOperationException($"Falha ao enviar diretório {AppConfig.Current.Shards.RemoteDir}.");

                uploadTask.Value = uploadTask.MaxValue;
            });

        AnsiConsole.MarkupLine(
            $"[green]✓ Shards processados[/] [grey](total: {allPrefixes.Count}, reuso local/upload: {releasePlan.PrefixesUploadOnly.Count}, regenerados: {releasePlan.PrefixesToGenerate.Count}, vazios: {emptyCount}, workers: {workerCount}, batch: {batchSize})[/]");
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
            tempIndexPath);

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
            .Where(prefix => _shardQueryBuilder.HasPartitionData("estabelecimento", prefix))
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
        var writers = new Dictionary<string, BinaryIndexedShardWriter>(StringComparer.Ordinal);

        try
        {
            foreach (var prefix in availablePrefixes)
            {
                var writer = new BinaryIndexedShardWriter(
                    tempDataFiles[prefix],
                    tempIndexFiles[prefix]);
                writers[prefix] = writer;
            }

            var query = _shardQueryBuilder.BuildJsonQueryForPrefixBatch(availablePrefixes, includeCnpjColumn: true, jsonAlias: "json_data");
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
            var exportQuery = _shardQueryBuilder.BuildJsonQueryForCnpj(prefixStr, cnpjBasico, cnpjOrdem, cnpjDv, jsonAlias: "json_output");

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
    public async Task GenerateAndUploadFinalInfoJsonAsync(
        string releaseId,
        IReadOnlyList<DataIntegrationRunSummary>? integrationSummaries = null,
        string? receitaLastUpdated = null)
    {
        try
        {
            // Garante que as views estejam disponíveis para contagem
            await LoadParquetTablesForConnection(_connection);

            var total = CountShardRecordsFromIndexes(AppConfig.Current.Paths.OutputDir, releaseId);

            var lastUpdated = string.IsNullOrWhiteSpace(receitaLastUpdated)
                ? DateTime.UtcNow.ToString("o")
                : receitaLastUpdated;

            var payload = new
            {
                total = total,
                last_updated = lastUpdated,
                zip_available = false,
                zip_size = 0L,
                zip_url = "",
                zip_md5checksum = "",
                shard_prefix_length = AppConfig.Current.Shards.PrefixLength,
                shard_count = _shardQueryBuilder.GetShardCount(),
                storage_release_id = releaseId,
                datasets = new Dictionary<string, object>(StringComparer.Ordinal)
                {
                    ["receita"] = new
                    {
                        storage_release_id = releaseId,
                        updated_at = lastUpdated,
                        record_count = total
                    }
                },
                shard_index_distribution = "r2",
                shard_format = "ndjson+binary-index",
                zip_layout = "disabled",
                cnpj_type = "string"
            };

            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
            {
                PropertyNamingPolicy = null,
                WriteIndented = false
            });

            var outputDir = GetLocalReleaseOutputDir(AppConfig.Current.Paths.OutputDir, releaseId);
            Directory.CreateDirectory(outputDir);
            var localInfoPath = Path.Combine(outputDir, "info.json");
            await File.WriteAllTextAsync(localInfoPath, json, Utf8NoBom);

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
        string indexPath)
    {
        var query = _shardQueryBuilder.BuildJsonQueryForPrefix(prefixStr, includeCnpjColumn: true, jsonAlias: "json_data");

        if (query is null)
            return 0;

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = query;

        await using var reader = await cmd.ExecuteReaderAsync();
        using var writer = new BinaryIndexedShardWriter(outputPath, indexPath);

        while (await reader.ReadAsync())
        {
            await writer.AppendAsync(reader.GetString(0), reader.GetString(1));
        }

        await writer.FlushAsync();

        if (writer.RecordCount == 0)
            return 0;
        return writer.RecordCount;
    }

    private static string? ResolveDatasetKey(string? datasetKey)
    {
        if (DatasetPathResolver.IsDatasetKey(datasetKey))
            return datasetKey;

        return DatasetPathResolver.ResolveLatestLocalDatasetKey(AppConfig.Current.Paths);
    }

    private static string BuildReleaseShardRemoteDir(string releaseId)
    {
        return $"shards/releases/{releaseId.Trim('/')}";
    }

    private static ShardReleasePlan BuildReleasePlan(
        string localShardDir,
        IReadOnlyList<string> allPrefixes)
    {
        var prefixesUploadOnly = new List<string>();
        var prefixesToGenerate = new List<string>();

        foreach (var prefix in allPrefixes)
        {
            if (File.Exists(Path.Combine(localShardDir, $"{prefix}{ShardDataExtension}"))
                && File.Exists(Path.Combine(localShardDir, $"{prefix}{ShardIndexExtension}")))
            {
                prefixesUploadOnly.Add(prefix);
            }
            else
            {
                prefixesToGenerate.Add(prefix);
            }
        }

        return new ShardReleasePlan(
            prefixesUploadOnly,
            prefixesToGenerate);
    }

    internal static (IReadOnlyList<string> UploadOnly, IReadOnlyList<string> ToGenerate) BuildReleasePlanForTest(
        string localShardDir,
        IReadOnlyList<string> allPrefixes)
    {
        var plan = BuildReleasePlan(localShardDir, allPrefixes);
        return (plan.PrefixesUploadOnly, plan.PrefixesToGenerate);
    }

    internal static Encoding InfoJsonEncodingForTest => Utf8NoBom;

    internal long CountShardRecordsFromIndexesForTest(string outputRootDir, string releaseId) =>
        CountShardRecordsFromIndexes(outputRootDir, releaseId);

    internal long CountShardRecordsFromIndexesForPublication(string outputRootDir, string releaseId) =>
        CountShardRecordsFromIndexes(outputRootDir, releaseId);

    private long CountShardRecordsFromIndexes(string outputRootDir, string releaseId)
    {
        var shardDir = Path.Combine(GetLocalReleaseOutputDir(outputRootDir, releaseId), AppConfig.Current.Shards.RemoteDir);
        return CountShardRecordsFromIndexDirectory(shardDir);
    }

    internal static long CountShardRecordsFromIndexDirectoryForTest(string shardDir) =>
        CountShardRecordsFromIndexDirectory(shardDir);

    private static long CountShardRecordsFromIndexDirectory(string shardDir)
    {
        if (!Directory.Exists(shardDir))
            return 0;

        long total = 0;
        Span<byte> header = stackalloc byte[BinaryIndexedShardWriter.HeaderSize];

        foreach (var indexPath in Directory.EnumerateFiles(shardDir, $"*{ShardIndexExtension}", SearchOption.TopDirectoryOnly))
        {
            using var stream = File.OpenRead(indexPath);
            if (stream.Length < BinaryIndexedShardWriter.HeaderSize)
                continue;

            var read = stream.Read(header);
            if (read != BinaryIndexedShardWriter.HeaderSize
                || !header[..4].SequenceEqual("OCI1"u8))
            {
                continue;
            }

            total += BinaryPrimitives.ReadUInt32LittleEndian(header[4..]);
        }

        return total;
    }

    private static IReadOnlyList<string> BuildUploadTargets(
        string localShardDir,
        IEnumerable<string> prefixes) =>
        prefixes
            .Distinct(StringComparer.Ordinal)
            .OrderBy(prefix => prefix, StringComparer.Ordinal)
            .SelectMany(prefix => new[]
            {
                $"{prefix}{ShardDataExtension}",
                $"{prefix}{ShardIndexExtension}"
            })
            .Where(path => File.Exists(Path.Combine(localShardDir, path)))
            .ToArray();

    private string GetDatasetOutputDir(string outputRootDir)
    {
        return DatasetPathResolver.GetDatasetPath(outputRootDir, _datasetKey);
    }

    private string GetLocalReleaseOutputDir(string outputRootDir, string releaseId)
    {
        return Path.Combine(GetDatasetOutputDir(outputRootDir), "releases", releaseId.Trim('/'));
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

    private async Task<DuckDBConnection> CreateConfiguredConnectionAsync()
    {
        var connection = new DuckDBConnection($"Data Source={_dataSource}");
        await connection.OpenAsync();
        ConfigureDuckDb(connection);
        return connection;
    }

    internal int GetShardCountFromFilesystemForPublication() => _shardQueryBuilder.GetShardCount();

    private static List<List<string>> BuildShardPrefixBatches(IReadOnlyList<string> prefixes, int batchSize)
    {
        var batches = new List<List<string>>();
        for (var index = 0; index < prefixes.Count; index += batchSize)
        {
            batches.Add(prefixes.Skip(index).Take(batchSize).ToList());
        }

        return batches;
    }

    public void Dispose()
    {
        _connection?.Dispose();
    }
}
