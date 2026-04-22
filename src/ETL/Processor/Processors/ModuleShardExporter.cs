using System.Text.Json;
using System.Text.Json.Nodes;
using System.Diagnostics;
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

    internal async Task<ModuleShardExportResult> ExportLocalAsync(
        DataIntegrationShardSource source,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        ValidateSource(source);

        var localShardDir = Path.Combine(
            outputRootDir,
            "shards",
            "modules",
            source.Key,
            "releases",
            releaseId.Trim('/'));
        Directory.CreateDirectory(localShardDir);

        AnsiConsole.MarkupLine(
            $"[cyan]Lendo prefixos do módulo {source.Key.EscapeMarkup()} a partir de {source.ParquetGlob.EscapeMarkup()}...[/]");
        var prefixes = await LoadAllPrefixesAsync(source.ParquetGlob, cancellationToken);

        if (prefixes.Count == 0)
        {
            AnsiConsole.MarkupLine($"[yellow]⚠️ Nenhum prefixo encontrado para o módulo {source.Key.EscapeMarkup()}[/]");
            return new ModuleShardExportResult(localShardDir, []);
        }

        AnsiConsole.MarkupLine(
            $"[grey]Módulo {source.Key.EscapeMarkup()}:[/] [cyan]{prefixes.Count} prefixos[/] serão materializados no release [cyan]{releaseId.EscapeMarkup()}[/]");
        var generatedPrefixes = await WriteModuleShardsAsync(
            source.Key,
            source.ParquetGlob,
            prefixes,
            localShardDir,
            cancellationToken);

        AnsiConsole.MarkupLine(
            $"[green]✓ Shards do módulo {source.Key.EscapeMarkup()} gerados[/] [grey](prefixos solicitados: {prefixes.Count}, com dados: {generatedPrefixes.Count})[/]");

        return new ModuleShardExportResult(localShardDir, generatedPrefixes);
    }

    private static async Task<IReadOnlyList<string>> LoadAllPrefixesAsync(
        string parquetPath,
        CancellationToken cancellationToken)
    {
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT DISTINCT cnpj_prefix
            FROM read_parquet('{EscapeSqlLiteral(parquetPath)}')
            WHERE cnpj_prefix IS NOT NULL AND cnpj_prefix <> ''
            ORDER BY cnpj_prefix";

        var prefixes = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            prefixes.Add(reader.GetString(0));

        return prefixes;
    }

    private static async Task<IReadOnlyList<string>> WriteModuleShardsAsync(
        string moduleKey,
        string parquetPath,
        IReadOnlyList<string> prefixes,
        string localShardDir,
        CancellationToken cancellationToken)
    {
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var prefixListSql = string.Join(", ", prefixes.Select(prefix => $"'{EscapeSqlLiteral(prefix)}'"));
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT cnpj_prefix, cnpj, payload_json
            FROM read_parquet('{EscapeSqlLiteral(parquetPath)}')
            WHERE cnpj_prefix IN ({prefixListSql})
              AND cnpj IS NOT NULL
              AND payload_json IS NOT NULL
            ORDER BY cnpj_prefix, cnpj";

        var writerByPrefix = new Dictionary<string, BinaryIndexedShardWriter>(StringComparer.Ordinal);
        var generatedPrefixes = new SortedSet<string>(StringComparer.Ordinal);
        var processedRecords = 0L;
        var progressStopwatch = Stopwatch.StartNew();
        var lastReported = TimeSpan.Zero;
        string? lastPrefix = null;

        AnsiConsole.MarkupLine(
            $"[cyan]Materializando shards do módulo {moduleKey.EscapeMarkup()}...[/] [grey](prefixos: {prefixes.Count})[/]");

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var prefix = reader.GetString(0);
                var cnpj = reader.GetString(1);
                var payloadJson = NormalizePayloadJson(cnpj, reader.GetString(2));
                lastPrefix = prefix;

                if (!writerByPrefix.TryGetValue(prefix, out var writer))
                {
                    writer = new BinaryIndexedShardWriter(
                        Path.Combine(localShardDir, $"{prefix}{ShardDataExtension}.tmp"),
                        Path.Combine(localShardDir, $"{prefix}{ShardIndexExtension}.tmp"));
                    writerByPrefix[prefix] = writer;

                    if (writerByPrefix.Count == 1 || writerByPrefix.Count % 100 == 0)
                    {
                        AnsiConsole.MarkupLine(
                            $"[grey]Módulo {moduleKey.EscapeMarkup()}:[/] [cyan]{writerByPrefix.Count}[/] prefixos com dados encontrados até agora [grey](último: {prefix.EscapeMarkup()})[/]");
                    }
                }

                await writer.AppendAsync(cnpj, payloadJson);
                generatedPrefixes.Add(prefix);
                processedRecords++;

                if (processedRecords == 1
                    || processedRecords % 100_000 == 0
                    || progressStopwatch.Elapsed - lastReported >= TimeSpan.FromSeconds(30))
                {
                    lastReported = progressStopwatch.Elapsed;
                    AnsiConsole.MarkupLine(
                        $"[grey]Módulo {moduleKey.EscapeMarkup()}:[/] [cyan]{processedRecords:N0}[/] registros serializados [grey](prefixos com dados: {generatedPrefixes.Count}, último prefixo: {(lastPrefix ?? "-").EscapeMarkup()}, tempo: {progressStopwatch.Elapsed:hh\\:mm\\:ss})[/]");
                }
            }

            foreach (var writer in writerByPrefix.Values)
                await writer.FlushAsync();
        }
        finally
        {
            foreach (var writer in writerByPrefix.Values)
                writer.Dispose();
        }

        foreach (var prefix in prefixes)
        {
            var tempData = Path.Combine(localShardDir, $"{prefix}{ShardDataExtension}.tmp");
            var tempIndex = Path.Combine(localShardDir, $"{prefix}{ShardIndexExtension}.tmp");
            var finalData = Path.Combine(localShardDir, $"{prefix}{ShardDataExtension}");
            var finalIndex = Path.Combine(localShardDir, $"{prefix}{ShardIndexExtension}");

            DeleteIfExists(finalData);
            DeleteIfExists(finalIndex);

            if (!generatedPrefixes.Contains(prefix))
            {
                DeleteIfExists(tempData);
                DeleteIfExists(tempIndex);
                continue;
            }

            File.Move(tempData, finalData);
            File.Move(tempIndex, finalIndex);
        }

        AnsiConsole.MarkupLine(
            $"[grey]Módulo {moduleKey.EscapeMarkup()}:[/] finalização concluída [grey](registros: {processedRecords:N0}, prefixos com dados: {generatedPrefixes.Count}, tempo total: {progressStopwatch.Elapsed:hh\\:mm\\:ss})[/]");

        return generatedPrefixes.ToArray();
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

        if (!File.Exists(source.ParquetGlob))
            throw new FileNotFoundException("Parquet da integração não encontrado.", source.ParquetGlob);
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
