using System.Text.Json;
using System.Text.Json.Nodes;
using CNPJExporter.Exporters;
using CNPJExporter.Integrations;
using CNPJExporter.Processors.Models;
using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class ModuleShardExporter
{
    private const string ShardDataExtension = ".ndjson";
    private const string ShardIndexExtension = ".index.bin";

    public async Task<ModuleShardExportResult> ExportAndUploadAsync(
        DataIntegrationShardSource source,
        string releaseId,
        string outputRootDir,
        IReadOnlyCollection<string>? prefixesToRegenerate,
        CancellationToken cancellationToken = default)
    {
        var result = await ExportLocalAsync(source, releaseId, outputRootDir, prefixesToRegenerate, cancellationToken);
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
        IReadOnlyCollection<string>? prefixesToRegenerate,
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

        var prefixes = prefixesToRegenerate is null
            ? await LoadAllPrefixesAsync(source.ParquetGlob, cancellationToken)
            : prefixesToRegenerate
                .Where(prefix => !string.IsNullOrWhiteSpace(prefix))
                .Select(prefix => prefix.Trim())
                .Distinct(StringComparer.Ordinal)
                .OrderBy(prefix => prefix, StringComparer.Ordinal)
                .ToArray();

        if (prefixes.Count == 0)
        {
            return new ModuleShardExportResult(localShardDir, []);
        }

        var generatedPrefixes = await WriteModuleShardsAsync(
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

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
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
                }

                await writer.AppendAsync(cnpj, payloadJson);
                generatedPrefixes.Add(prefix);
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
        $"shards/modules/{moduleKey.Trim('/')}/releases/{releaseId.Trim('/')}";

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
