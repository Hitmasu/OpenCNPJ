using System.Data;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Diagnostics;
using CNPJExporter.Configuration;
using CNPJExporter.Exporters;
using CNPJExporter.Integrations;
using CNPJExporter.Processors.Models;
using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class ModuleShardExporter : IModuleShardExporter
{
    private const string ShardDataExtension = ".ndjson";
    private const string ShardIndexExtension = ".index.bin";
    internal const int MaxOpenShardWriters = 16;

    public async Task<ModuleShardExportResult> ExportAndUploadAsync(
        DataIntegrationShardSource source,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        var result = await ExportLocalAsync(source, releaseId, outputRootDir, cancellationToken);
        var uploadTargets = BuildUploadTargets(result.LocalShardDir, result.GeneratedPrefixes);

        if (uploadTargets.Count == 0)
            return result;

        var remoteDir = BuildModuleShardRemoteDir(source.Key, releaseId);
        var uploaded = await RcloneClient.UploadSelectedFilesAsync(result.LocalShardDir, remoteDir, uploadTargets);
        if (!uploaded)
            throw new InvalidOperationException($"Falha ao enviar shards do módulo {source.Key}.");

        return result;
    }

    public async Task<ModuleShardExportResult> ExportSegmentAndUploadAsync(
        DataIntegrationShardSource source,
        DataIntegrationSegment segment,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        ValidateSegment(segment);
        var localShardDir = Path.Combine(
            outputRootDir,
            "shards",
            "modules",
            source.Key,
            "segments",
            segment.Id,
            "releases",
            releaseId.Trim('/'));
        var result = await ExportLocalFromParquetAsync(
            source.Key,
            segment.ParquetGlob,
            localShardDir,
            AppConfig.Current.PortalTransparenciaIntegration.DuckDbMemoryLimit,
            cancellationToken);
        var uploadTargets = BuildUploadTargets(
            result.LocalShardDir,
            result.GeneratedPrefixes);
        if (uploadTargets.Count == 0)
            return result;

        var remoteDir = BuildModuleSegmentShardRemoteDir(
            source.Key,
            segment.Id,
            releaseId);
        var uploaded = await RcloneClient.UploadSelectedFilesAsync(
            result.LocalShardDir,
            remoteDir,
            uploadTargets);
        if (!uploaded)
        {
            throw new InvalidOperationException(
                $"Falha ao enviar o segmento {segment.Id} do módulo {source.Key}.");
        }

        return result;
    }

    internal async Task<ModuleShardExportResult> ExportLocalAsync(
        DataIntegrationShardSource source,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        ValidateSource(source);
        var parquetPath = source.ParquetGlob
                          ?? throw new InvalidOperationException(
                              $"O módulo {source.Key} não informou Parquet.");

        var localShardDir = Path.Combine(
            outputRootDir,
            "shards",
            "modules",
            source.Key,
            "releases",
            releaseId.Trim('/'));
        return await ExportLocalFromParquetAsync(
            source.Key,
            parquetPath,
            localShardDir,
            AppConfig.Current.DuckDb.MemoryLimit,
            cancellationToken);
    }

    private static async Task<ModuleShardExportResult>
        ExportLocalFromParquetAsync(
            string moduleKey,
            string parquetPath,
            string localShardDir,
            string duckDbMemoryLimit,
            CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(localShardDir);
        if (!ParquetGlobExists(parquetPath))
        {
            throw new FileNotFoundException(
                "Parquet da integração não encontrado.",
                parquetPath);
        }

        AnsiConsole.MarkupLine(
            $"[cyan]Lendo e materializando o módulo {moduleKey.EscapeMarkup()} a partir de {parquetPath.EscapeMarkup()}...[/]");
        var generatedPrefixes = await WriteModuleShardsAsync(
            moduleKey,
            parquetPath,
            localShardDir,
            duckDbMemoryLimit,
            cancellationToken);

        if (generatedPrefixes.Count == 0)
        {
            AnsiConsole.MarkupLine(
                $"[yellow]⚠️ Nenhum prefixo encontrado para o módulo {moduleKey.EscapeMarkup()}[/]");
        }
        else
        {
            AnsiConsole.MarkupLine(
                $"[green]✓ Shards do módulo {moduleKey.EscapeMarkup()} gerados[/] [grey](prefixos: {generatedPrefixes.Count})[/]");
        }

        return new ModuleShardExportResult(localShardDir, generatedPrefixes);
    }

    private static async Task<IReadOnlyList<string>> WriteModuleShardsAsync(
        string moduleKey,
        string parquetPath,
        string localShardDir,
        string duckDbMemoryLimit,
        CancellationToken cancellationToken)
    {
        var duckDbTempDirectory = Path.Combine(localShardDir, ".duckdb-temp");
        DeleteDirectoryIfExists(duckDbTempDirectory);
        foreach (var tempFile in Directory.EnumerateFiles(localShardDir, "*.tmp"))
            DeleteIfExists(tempFile);
        Directory.CreateDirectory(duckDbTempDirectory);

        var generatedPrefixes = new SortedSet<string>(StringComparer.Ordinal);
        var writerByPrefix = new Dictionary<string, BinaryIndexedShardWriter>(StringComparer.Ordinal);
        var openWriters = new Queue<BinaryIndexedShardWriter>(MaxOpenShardWriters);
        var processedRecords = 0L;
        var progressStopwatch = Stopwatch.StartNew();
        var lastReported = TimeSpan.Zero;

        AnsiConsole.MarkupLine(
            $"[cyan]Materializando shards do módulo {moduleKey.EscapeMarkup()} em streaming...[/]");

        try
        {
            await using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync();
            await ConfigureDuckDbAsync(
                connection,
                duckDbMemoryLimit,
                duckDbTempDirectory,
                cancellationToken);

            await using var cmd = connection.CreateCommand();
            cmd.UseStreamingMode = true;
            cmd.CommandText = $@"
                SELECT cnpj_prefix, cnpj, payload_json
                FROM read_parquet('{EscapeSqlLiteral(parquetPath)}')
                WHERE cnpj_prefix IS NOT NULL
                  AND cnpj_prefix <> ''
                  AND cnpj IS NOT NULL
                  AND payload_json IS NOT NULL";

            try
            {
                await using var reader = await cmd.ExecuteReaderAsync(
                    CommandBehavior.SequentialAccess,
                    cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var prefix = reader.GetString(0);
                    var cnpj = reader.GetString(1);
                    var payloadJson = NormalizePayloadJson(cnpj, reader.GetString(2));

                    if (!writerByPrefix.TryGetValue(prefix, out var writer))
                    {
                        writer = new BinaryIndexedShardWriter(
                            Path.Combine(localShardDir, $"{prefix}{ShardDataExtension}.tmp"),
                            Path.Combine(localShardDir, $"{prefix}{ShardIndexExtension}.tmp"));
                        writerByPrefix[prefix] = writer;
                        generatedPrefixes.Add(prefix);

                        if (generatedPrefixes.Count == 1
                            || generatedPrefixes.Count % 100 == 0)
                        {
                            AnsiConsole.MarkupLine(
                                $"[grey]Módulo {moduleKey.EscapeMarkup()}:[/] [cyan]{generatedPrefixes.Count}[/] prefixos com dados encontrados até agora [grey](último: {prefix.EscapeMarkup()})[/]");
                        }
                    }

                    if (!writer.IsOpen)
                    {
                        if (openWriters.Count == MaxOpenShardWriters)
                            await openWriters.Dequeue().SuspendAsync();

                        openWriters.Enqueue(writer);
                    }

                    await writer.AppendAsync(cnpj, payloadJson);
                    processedRecords++;

                    if (processedRecords == 1
                        || processedRecords % 100_000 == 0
                        || progressStopwatch.Elapsed - lastReported >= TimeSpan.FromSeconds(30))
                    {
                        lastReported = progressStopwatch.Elapsed;
                        AnsiConsole.MarkupLine(
                            $"[grey]Módulo {moduleKey.EscapeMarkup()}:[/] [cyan]{processedRecords:N0}[/] registros serializados [grey](prefixos com dados: {generatedPrefixes.Count}, último prefixo: {prefix.EscapeMarkup()}, tempo: {progressStopwatch.Elapsed:hh\\:mm\\:ss})[/]");
                    }
                }

                foreach (var (prefix, writer) in writerByPrefix.OrderBy(
                             pair => pair.Key,
                             StringComparer.Ordinal))
                {
                    await CompleteShardAsync(
                        writer,
                        prefix,
                        localShardDir);
                }
            }
            finally
            {
                foreach (var writer in writerByPrefix.Values)
                    writer.Dispose();
            }
        }
        finally
        {
            DeleteDirectoryIfExists(duckDbTempDirectory);
        }

        AnsiConsole.MarkupLine(
            $"[grey]Módulo {moduleKey.EscapeMarkup()}:[/] finalização concluída [grey](registros: {processedRecords:N0}, prefixos com dados: {generatedPrefixes.Count}, tempo total: {progressStopwatch.Elapsed:hh\\:mm\\:ss})[/]");

        return generatedPrefixes.ToArray();
    }

    private static async Task CompleteShardAsync(
        BinaryIndexedShardWriter writer,
        string prefix,
        string localShardDir)
    {
        try
        {
            await writer.FlushAsync();
        }
        finally
        {
            writer.Dispose();
        }

        var tempData = Path.Combine(
            localShardDir,
            $"{prefix}{ShardDataExtension}.tmp");
        var tempIndex = Path.Combine(
            localShardDir,
            $"{prefix}{ShardIndexExtension}.tmp");
        var finalData = Path.Combine(
            localShardDir,
            $"{prefix}{ShardDataExtension}");
        var finalIndex = Path.Combine(
            localShardDir,
            $"{prefix}{ShardIndexExtension}");

        DeleteIfExists(finalData);
        DeleteIfExists(finalIndex);
        File.Move(tempData, finalData);
        File.Move(tempIndex, finalIndex);
    }

    private static async Task ConfigureDuckDbAsync(
        DuckDBConnection connection,
        string memoryLimit,
        string tempDirectory,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(memoryLimit))
            throw new InvalidOperationException("O limite de memória do DuckDB é obrigatório.");

        var maxTempDirectorySize = AppConfig.Current
            .PortalTransparenciaIntegration
            .DuckDbMaxTempDirectorySize;
        if (string.IsNullOrWhiteSpace(maxTempDirectorySize))
        {
            throw new InvalidOperationException(
                "O limite do diretório temporário do DuckDB é obrigatório.");
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SET preserve_insertion_order = false;
            SET threads = 1;
            SET memory_limit = '{EscapeSqlLiteral(memoryLimit)}';
            SET temp_directory = '{EscapeSqlLiteral(tempDirectory)}';
            SET max_temp_directory_size = '{EscapeSqlLiteral(maxTempDirectorySize)}';";
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static IReadOnlyCollection<string> BuildUploadTargets(
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

    private static string BuildModuleShardRemoteDir(string moduleKey, string releaseId) =>
        $"shards/modules/{moduleKey.Trim('/')}/{releaseId.Trim('/')}";

    private static string BuildModuleSegmentShardRemoteDir(
        string moduleKey,
        string segmentId,
        string releaseId) =>
        $"shards/modules/{moduleKey.Trim('/')}/segments/{segmentId}/{releaseId.Trim('/')}";

    private static string NormalizePayloadJson(string cnpj, string payloadJson)
    {
        using var document = JsonDocument.Parse(payloadJson);
        if (document.RootElement.ValueKind != JsonValueKind.String)
            return EnsureCnpjProperty(cnpj, document.RootElement.GetRawText());

        var nestedJson = document.RootElement.GetString();
        if (string.IsNullOrWhiteSpace(nestedJson))
            throw new InvalidOperationException("Payload da integração não pode ser nulo ou vazio.");

        using var nestedDocument = JsonDocument.Parse(nestedJson);
        return EnsureCnpjProperty(cnpj, nestedDocument.RootElement.GetRawText());
    }

    private static string EnsureCnpjProperty(string cnpj, string payloadJson)
    {
        var node = JsonNode.Parse(payloadJson) as JsonObject
            ?? throw new InvalidOperationException("Payload da integração deve ser um objeto JSON.");

        node["cnpj"] = cnpj;
        return node.ToJsonString();
    }

    private static void ValidateSource(DataIntegrationShardSource source)
    {
        var descriptor = new DataIntegrationDescriptor(
            source.Key,
            source.JsonPropertyName,
            TimeSpan.FromHours(1),
            source.SchemaVersion);
        descriptor.Validate();

        if (string.IsNullOrWhiteSpace(source.ParquetGlob))
            throw new ArgumentException("O Parquet da integração é obrigatório.", nameof(source));

        if (!ParquetGlobExists(source.ParquetGlob))
            throw new FileNotFoundException("Parquet da integração não encontrado.", source.ParquetGlob);
    }

    private static void ValidateSegment(DataIntegrationSegment segment)
    {
        if (!TryParseSegmentId(segment.Id, out _))
        {
            throw new ArgumentException(
                "O segmento deve usar YYYY ou YYYY-MM.",
                nameof(segment));
        }

        if (string.IsNullOrWhiteSpace(segment.ParquetGlob)
            || !ParquetGlobExists(segment.ParquetGlob))
        {
            throw new FileNotFoundException(
                $"Parquet do segmento {segment.Id} não encontrado.",
                segment.ParquetGlob);
        }
    }

    private static bool ParquetGlobExists(string parquetGlob)
    {
        if (!parquetGlob.Contains('*', StringComparison.Ordinal)
            && !parquetGlob.Contains('?', StringComparison.Ordinal))
        {
            return File.Exists(parquetGlob);
        }

        var wildcard = parquetGlob.IndexOfAny(['*', '?']);
        var directoryEnd = parquetGlob.LastIndexOfAny(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            wildcard);
        var directory = directoryEnd < 0 ? "." : parquetGlob[..directoryEnd];
        if (!Directory.Exists(directory))
            return false;

        return Directory.EnumerateFiles(
                directory,
                "*.parquet",
                SearchOption.TopDirectoryOnly)
            .Any();
    }

    internal static bool TryParseSegmentId(
        string segmentId,
        out DateOnly period)
    {
        period = default;
        if (segmentId.Length == 4
            && int.TryParse(
                segmentId,
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out var year)
            && year is >= 1 and <= 9999)
        {
            period = new DateOnly(year, 1, 1);
            return true;
        }

        if (segmentId.Length != 7
            || segmentId[4] != '-'
            || !int.TryParse(
                segmentId.AsSpan(0, 4),
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out year)
            || !int.TryParse(
                segmentId.AsSpan(5, 2),
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out var month)
            || year is < 1 or > 9999
            || month is < 1 or > 12)
        {
            return false;
        }

        period = new DateOnly(year, month, 1);
        return true;
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, true);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
