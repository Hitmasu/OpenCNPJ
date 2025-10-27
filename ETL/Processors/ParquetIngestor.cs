using System.Collections.Concurrent;
using System.IO.Compression;
using System.Text;
using CNPJExporter.Configuration;
using CNPJExporter.Exporters;
using CNPJExporter.Utils;
using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Processors;

public class ParquetIngestor : IDisposable
{
    private readonly string _dataDir;
    private readonly string _parquetDir;
    private readonly DuckDBConnection _connection;
    private readonly NdjsonProcessor _ndjsonProcessor;
    private readonly SemaphoreSlim _connectionSemaphore = new(1, 1);

    public ParquetIngestor()
    {
        _dataDir = AppConfig.Current.Paths.DataDir;
        _parquetDir = AppConfig.Current.Paths.ParquetDir;
        Directory.CreateDirectory(_parquetDir);

        var dataSource = AppConfig.Current.DuckDb.UseInMemory ? ":memory:" : "./cnpj.duckdb";
        _connection = new($"Data Source={dataSource}");
        _connection.Open();

        _ndjsonProcessor = new();

        ConfigureDuckDb();

        AnsiConsole.MarkupLine("[green]ParquetIngestor inicializado com DuckDB (otimizado)[/]");
    }

    private void ConfigureDuckDb()
    {
        try
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                PRAGMA threads = {Math.Max(AppConfig.Current.DuckDb.ThreadsPragma, Environment.ProcessorCount)};
                SET memory_limit = '{AppConfig.Current.DuckDb.MemoryLimit}';
                SET threads = {Math.Max(AppConfig.Current.DuckDb.EngineThreads, Environment.ProcessorCount)};
                
                PRAGMA temp_directory='./temp';
                PRAGMA enable_progress_bar=false;
                PRAGMA force_index_join=true;
                PRAGMA enable_object_cache=true;
                SET parallel_csv_read=true;
                SET preserve_insertion_order=false;
            ";
            cmd.ExecuteNonQuery();

            using var cmd4 = _connection.CreateCommand();
            cmd4.CommandText =
                $"SET preserve_insertion_order = {(AppConfig.Current.DuckDb.PreserveInsertionOrder ? "true" : "false")}";
            cmd4.ExecuteNonQuery();

            AnsiConsole.MarkupLine("[green]✓ Configurações de performance aplicadas[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[yellow]Aviso ao configurar performance: {ex.Message.EscapeMarkup()}[/]");
        }
    }

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

                    // Verifica existência prévia dos Parquet para pular processamento
                    var parquetPath = Path.Combine(_parquetDir, $"{tableName}.parquet");
                    var partitionedDir = Path.Combine(_parquetDir, tableName);

                    var isPartitioned = new[] { "estabelecimento", "empresa", "simples", "socio" }.Contains(tableName);
                    var parquetAlreadyExists = isPartitioned
                        ? Directory.Exists(partitionedDir) && Directory
                            .EnumerateFiles(partitionedDir, "*.parquet", SearchOption.AllDirectories).Any()
                        : File.Exists(parquetPath);

                    if (parquetAlreadyExists)
                    {
                        task.Description = $"[blue]Pulando {tableName}: Parquet já existe[/]";
                        task.Value = task.MaxValue;
                        continue;
                    }

                    task.MaxValue = files.Length;
                    await ConvertTableToParquet(tableName, files, columns, task);
                }
            });
    }

    private async Task ConvertTableToParquet(string tableName, string[] csvFiles, string[] columns, ProgressTask task)
    {
        var parquetPath = Path.Combine(_parquetDir, $"{tableName}.parquet");
        var partitionedDir = Path.Combine(_parquetDir, tableName);

        string[] tables = ["estabelecimento", "empresa", "simples", "socio"];

        var tempTableName = $"temp_{tableName}_{Guid.NewGuid():N}";

        try
        {
            var createTableSql = $@"
                CREATE TEMPORARY TABLE {tempTableName} (
                    {string.Join(", ", columns.Select(c => $"{c} VARCHAR"))}
                )";

            await using var createCmd = _connection.CreateCommand();
            createCmd.CommandText = createTableSql;
            await createCmd.ExecuteNonQueryAsync();

            var batchSize = AppConfig.Current.Ndjson.BatchUploadSize;

            for (var i = 0; i < csvFiles.Length; i += batchSize)
            {
                var batch = csvFiles.Skip(i).Take(batchSize).ToArray();

                foreach (var csvFile in batch)
                {
                    try
                    {
                        var insertSql = $@"
                            INSERT INTO {tempTableName} 
                            SELECT * FROM read_csv('{csvFile}', 
                                sep=';', 
                                header=false, 
                                encoding='CP1252',
                                ignore_errors=true,
                                quote='""',
                                escape='""',
                                max_line_size=10000000,
                                columns={{{string.Join(", ", columns.Select((c, i) => $"'{c}': 'VARCHAR'"))}}})";

                        await using var insertCmd = _connection.CreateCommand();
                        insertCmd.CommandText = insertSql;
                        await insertCmd.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        AnsiConsole.MarkupLine($"[red]Erro processando {csvFile}: {ex.Message.EscapeMarkup()}[/]");
                    }

                    task.Increment(1);
                }
            }

            if (tables.Contains(tableName))
            {
                Directory.CreateDirectory(partitionedDir);

                var exportSql = $@"
                    COPY (
                        SELECT *, 
                               SUBSTRING(cnpj_basico, 1, 2) as cnpj_prefix
                        FROM {tempTableName}
                    ) 
                    TO '{partitionedDir}' 
                    (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (cnpj_prefix), OVERWRITE)";

                await using var exportCmd = _connection.CreateCommand();
                exportCmd.CommandText = exportSql;
                await exportCmd.ExecuteNonQueryAsync();

                AnsiConsole.MarkupLine($"[green]✓ {tableName} particionado por CNPJ prefix criado[/]");
            }
            else
            {
                // Para tabelas de domínio, exporta arquivo único
                var exportSql = $@"
                    COPY (SELECT * FROM {tempTableName}) 
                    TO '{parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";

                await using var exportCmd = _connection.CreateCommand();
                exportCmd.CommandText = exportSql;
                await exportCmd.ExecuteNonQueryAsync();

                AnsiConsole.MarkupLine($"[green]✓ {tableName}.parquet criado[/]");
            }
        }
        finally
        {
            await using var dropCmd = _connection.CreateCommand();
            dropCmd.CommandText = $"DROP TABLE IF EXISTS {tempTableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }

    public async Task ExportAndUploadToStorage(string outputDir = "cnpj_ndjson")
    {
        Directory.CreateDirectory(outputDir);

        AnsiConsole.MarkupLine("[cyan]Carregando tabelas Parquet para memória...[/]");
        await LoadParquetTablesForConnection(_connection);

        AnsiConsole.MarkupLine("[cyan]🚀 Iniciando export + upload integrado para Storage...[/]");

        await ExportToNdjsonPartitioned(outputDir);

        AnsiConsole.MarkupLine("[green]🎉 Export + upload integrado concluído![/]");
    }

    private async Task LoadParquetTablesForConnection(DuckDBConnection connection)
    {
        var tableConfigs = new Dictionary<string, string>
        {
            ["empresa"] = "empresa/**/*.parquet",
            ["estabelecimento"] = "estabelecimento/**/*.parquet",
            ["socio"] = "socio/**/*.parquet",
            ["simples"] = "simples/**/*.parquet",
            ["cnae"] = "cnae.parquet",
            ["motivo"] = "motivo.parquet",
            ["municipio"] = "municipio.parquet",
            ["natureza"] = "natureza.parquet",
            ["pais"] = "pais.parquet",
            ["qualificacao"] = "qualificacao.parquet"
        };

        foreach (var (tableName, pattern) in tableConfigs)
        {
            try
            {
                var fullPath = Path.Combine(_parquetDir, pattern);
                var createViewSql = $"CREATE OR REPLACE VIEW {tableName} AS SELECT * FROM read_parquet('{fullPath}')";

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

    private async Task ExportToNdjsonPartitioned(string outputDir)
    {
        var prefixesToProcess = Enumerable.Range(0, 100).ToList();

        await AnsiConsole.Progress()
            .StartAsync(async ctx =>
            {
                var mainTask = ctx.AddTask("[green]Progresso geral[/]", maxValue: prefixesToProcess.Count);
                var taskDict = new ConcurrentDictionary<string, ProgressTask>();

                var parallelOptions = new ParallelOptions
                {
                    MaxDegreeOfParallelism = AppConfig.Current.Ndjson.MaxParallelProcessing > 0
                        ? AppConfig.Current.Ndjson.MaxParallelProcessing
                        : Environment.ProcessorCount
                };

                await Parallel.ForEachAsync(prefixesToProcess, parallelOptions, async (prefix, ct) =>
                {
                    var prefixStr = prefix.ToString("D2");
                    ProgressTask? prefixTask = null;

                    try
                    {
                        prefixTask = ctx.AddTask($"[yellow]Prefixo {prefixStr}[/]", autoStart: false);
                        taskDict[prefixStr] = prefixTask;
                        prefixTask.StartTask();

                        prefixTask.Description = $"[cyan]Gerando {prefixStr}.ndjson...[/]";
                        await ExportSinglePrefix(prefixStr, outputDir);

                        var ndjsonFile = Path.Combine(outputDir, $"{prefixStr}.ndjson");
                        if (File.Exists(ndjsonFile))
                        {
                            prefixTask.Description = $"[blue]Processando {prefixStr}.ndjson...[/]";
                            await _ndjsonProcessor.ProcessNdjsonFileToStorage(ndjsonFile, prefixTask);

                            File.Delete(ndjsonFile);
                            prefixTask.Description = $"[green]✓ {prefixStr}.ndjson concluído[/]";
                        }

                        mainTask.Increment(1);
                        prefixTask.StopTask();
                    }
                    catch (Exception ex)
                    {
                        if (prefixTask != null)
                        {
                            prefixTask.Description = $"[red]❌ Erro em {prefixStr}: {ex.Message.EscapeMarkup()}[/]";
                            prefixTask.StopTask();
                        }

                        AnsiConsole.WriteException(ex, new ExceptionSettings
                        {
                            Format = ExceptionFormats.ShortenEverything,
                            
                        });

                        mainTask.Increment(1);
                    }
                });
            });
        
        await HashCacheManager.UploadDatabaseAsync();
    }

    private async Task ExportSinglePrefix(string prefixStr, string outputDir)
    {
        try
        {
            var outputFile = Path.Combine(outputDir, $"{prefixStr}.ndjson");

            var exportQuery = BuildJsonQueryForPrefix(prefixStr, includeCnpjColumn: false, jsonAlias: "json_output");

            var copyQuery = $"COPY ({exportQuery}) TO '{outputFile}'";

            // Usa semáforo para serializar acesso à conexão DuckDB (não é thread-safe)
            await _connectionSemaphore.WaitAsync();
            try
            {
                await using var cmd = _connection.CreateCommand();
                cmd.CommandText = copyQuery;
                await cmd.ExecuteNonQueryAsync();
            }
            finally
            {
                _connectionSemaphore.Release();
            }

            var fileInfo = new FileInfo(outputFile);
            if (fileInfo is { Exists: true, Length: > 0 })
            {
                AnsiConsole.MarkupLine(
                    $"[green]✓ {prefixStr}.ndjson criado ({fileInfo.Length / 1024.0 / 1024.0:F1} MB)[/]");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Erro exportando prefixo {prefixStr}: {ex.Message.EscapeMarkup()}[/]");
        }
    }

    /// <summary>
    /// Exporta um CNPJ específico para arquivo JSON individual
    /// </summary>
    /// <param name="cnpj">CNPJ de 14 dígitos</param>
    /// <param name="outputDir">Diretório de saída</param>
    public async ValueTask ExportSingleCnpjAsync(string cnpj, string outputDir)
    {
        Directory.CreateDirectory(outputDir);

        AnsiConsole.MarkupLine("[cyan]Carregando tabelas Parquet para memória...[/]");
        await LoadParquetTablesForConnection(_connection);

        var cnpjBasico = cnpj[..8];
        var cnpjOrdem = cnpj.Substring(8, 4);
        var cnpjDv = cnpj.Substring(12, 2);
        var prefixStr = cnpjBasico[..2];

        AnsiConsole.MarkupLine($"[yellow]🎯 Buscando CNPJ {cnpj} (prefixo {prefixStr})...[/]");

        try
        {
            var outputFile = Path.Combine(outputDir, $"{cnpj}.json");
            var exportQuery = BuildJsonQueryForCnpj(cnpjBasico, cnpjOrdem, cnpjDv, jsonAlias: "json_output");

            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = exportQuery;

            var result = await cmd.ExecuteScalarAsync();

            if (result != null && result.ToString() != "")
            {
                var jsonContent = JsonCleanupUtils.CleanJsonSpaces(result.ToString());

                await File.WriteAllTextAsync(outputFile, jsonContent);

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

    /// <summary>
    /// Exporta todos os CNPJs diretamente para ZIP sem criar arquivos temporários em disco
    /// </summary>
    public async Task<string> ExportJsonsToZip(string outputDir)
    {
        await LoadParquetTablesForConnection(_connection);

        Directory.CreateDirectory(outputDir);
        var zipPath = Path.Combine(outputDir, $"cnpj_jsons_{DateTime.Now:yyyyMMdd_HHmmss}.zip");

        using var archive = ZipFile.Open(zipPath, ZipArchiveMode.Create);

        await AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .StartAsync("Exportando JSONs para ZIP...", async ctx =>
            {
                ctx.Status("Preparando exportação...");
                var prefixes = Enumerable.Range(0, 100).Select(i => i.ToString("00")).ToList();

                foreach (var prefix in prefixes)
                {
                    ctx.Status($"Exportando e compactando prefixo {prefix}...");
                    await ExportPrefixToZipDirectly(prefix, archive);
                }

                ctx.Status("Finalizando ZIP...");
            });

        var zipInfo = new FileInfo(zipPath);
        AnsiConsole.MarkupLine(
            $"[green]✓ ZIP criado: {zipPath} ({zipInfo.Length / 1024.0 / 1024.0 / 1024.0:F2} GB)[/]");
        return zipPath;
    }

    public async Task GenerateAndUploadFinalInfoJsonAsync(string zipPath)
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
            var zipInfo = new FileInfo(zipPath);
            var zipSize = zipInfo.Exists ? zipInfo.Length : 0L;

            string zipMd5Base64 = "";
            if (zipInfo.Exists)
            {
                using var md5 = System.Security.Cryptography.MD5.Create();
                await using var stream = File.OpenRead(zipPath);
                var hash = await md5.ComputeHashAsync(stream);
                zipMd5Base64 = Convert.ToBase64String(hash);
            }

            var payload = new
            {
                total = total,
                last_updated = lastUpdated,
                zip_size = zipSize,
                zip_url = "https://file.opencnpj.org/cnpjs.zip",
                zip_md5checksum = zipMd5Base64
            };

            var json = System.Text.Json.JsonSerializer.Serialize(payload, new System.Text.Json.JsonSerializerOptions
            {
                PropertyNamingPolicy = null,
                WriteIndented = false
            });

            var tempDir = Path.Combine(Path.GetTempPath(), "opencnpj_info");
            Directory.CreateDirectory(tempDir);
            var localInfoPath = Path.Combine(tempDir, "info.json");
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

    private async Task ExportPrefixToZipDirectly(string prefixStr, ZipArchive archive)
    {
        var query = BuildJsonQueryForPrefix(prefixStr, includeCnpjColumn: true, jsonAlias: "json_data");

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = query;

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var cnpj = reader.GetString(0);
            var jsonData = reader.GetString(1);

            var entry = archive.CreateEntry($"{cnpj}.json", CompressionLevel.Optimal);
            await using var entryStream = entry.Open();
            await using var writer = new StreamWriter(entryStream, Encoding.UTF8);
            await writer.WriteAsync(jsonData);
        }
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

    private string BuildJsonQueryForPrefix(string prefixStr, bool includeCnpjColumn, string jsonAlias)
    {
        var jsonFields = GetJsonStructFields();
        var selectCols = includeCnpjColumn
            ? "e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj, to_json(struct_pack(\n" + jsonFields +
              $"\n)) as {jsonAlias}"
            : $"to_json(struct_pack(\n" + jsonFields + $"\n)) as {jsonAlias}";

        return $@"WITH socios_data AS (
                SELECT 
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
                FROM socio s
                LEFT JOIN qualificacao qs ON s.qualificacao_socio = qs.codigo
                WHERE s.cnpj_prefix = '{prefixStr}'
                GROUP BY s.cnpj_basico
            )
            SELECT {selectCols}
            FROM estabelecimento e
            LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN socios_data sd ON e.cnpj_basico = sd.cnpj_basico
            WHERE e.cnpj_prefix = '{prefixStr}'";
    }

    private string BuildJsonQueryForCnpj(string cnpjBasico, string cnpjOrdem, string cnpjDv, string jsonAlias)
    {
        var jsonFields = GetJsonStructFields();
        var selectCols = $"to_json(struct_pack(\n" + jsonFields + $"\n)) as {jsonAlias}";

        return $@"WITH socios_data AS (
                SELECT 
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
                FROM socio s
                LEFT JOIN qualificacao qs ON s.qualificacao_socio = qs.codigo
                WHERE s.cnpj_basico = '{cnpjBasico}'
                GROUP BY s.cnpj_basico
            )
            SELECT {selectCols}
            FROM estabelecimento e
            LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN socios_data sd ON e.cnpj_basico = sd.cnpj_basico
            WHERE e.cnpj_basico = '{cnpjBasico}' 
              AND e.cnpj_ordem = '{cnpjOrdem}' 
              AND e.cnpj_dv = '{cnpjDv}'";
    }

    public void Dispose()
    {
        _connectionSemaphore?.Dispose();
        _connection?.Dispose();
        HashCacheManager.CloseConnections();
    }
}