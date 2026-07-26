using System.Buffers.Binary;
using System.Text.Json.Nodes;
using CNPJExporter.Integrations;
using CNPJExporter.Processors;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ModuleShardExporterTests
{
    [TestMethod]
    public async Task ExportLocalAsync_ShouldWriteSparseModuleShardWithBinaryIndex()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-module-shard-{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(tempRoot);
            var parquetPath = Path.Combine(tempRoot, "cno.parquet");
            await CreateIntegrationParquetAsync(parquetPath);

            var source = new DataIntegrationShardSource(
                "cno",
                "cno",
                "2",
                "test-source",
                DateTimeOffset.Parse("2026-04-14T00:00:00Z"),
                parquetPath,
                2);

            var result = await new ModuleShardExporter().ExportLocalAsync(
                source,
                "module-release",
                tempRoot);

            var dataPath = Path.Combine(result.LocalShardDir, "607.ndjson");
            var indexPath = Path.Combine(result.LocalShardDir, "607.index.bin");

            Assert.IsTrue(File.Exists(dataPath), "O shard sparse do módulo deveria ser gerado.");
            Assert.IsTrue(File.Exists(indexPath), "O índice binário do módulo deveria ser gerado.");
            CollectionAssert.AreEqual(new[] { "108", "607" }, result.GeneratedPrefixes.ToArray());

            var lines = await File.ReadAllLinesAsync(dataPath);
            Assert.AreEqual(1, lines.Length, "O shard do módulo deve conter apenas CNPJs com payload do módulo.");

            var line = JsonNode.Parse(lines[0])!.AsObject();
            Assert.AreEqual("60700007000148", line["cnpj"]!.GetValue<string>());
            Assert.IsTrue(line.ContainsKey("nome"), $"Payload do módulo sem campo nome. Linha atual: {lines[0]}");
            Assert.AreEqual("OBRA TESTE", line["nome"]!.GetValue<string>());

            var indexBytes = await File.ReadAllBytesAsync(indexPath);
            Assert.AreEqual(1u, BinaryPrimitives.ReadUInt32LittleEndian(indexBytes.AsSpan(4, sizeof(uint))));
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, true);
        }
    }

    [TestMethod]
    public async Task ExportLocalAsync_ShouldLimitOpenShardWriters()
    {
        const int prefixCount = 48;
        const int recordsPerPrefix = 2_000;
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-module-shard-bounded-{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(tempRoot);
            var parquetPath = Path.Combine(tempRoot, "many-prefixes.parquet");
            await CreateManyPrefixesParquetAsync(
                parquetPath,
                prefixCount,
                recordsPerPrefix);

            var source = new DataIntegrationShardSource(
                "bounded",
                "items",
                "1",
                "test-source",
                DateTimeOffset.Parse("2026-07-24T00:00:00Z"),
                parquetPath,
                prefixCount * recordsPerPrefix);

            if (!Directory.Exists("/proc/self/fd"))
                Assert.Inconclusive("A contagem de descritores requer /proc/self/fd.");

            var baselineOpenFileDescriptors = CountOpenFileDescriptors();
            var exportTask = new ModuleShardExporter().ExportLocalAsync(
                source,
                "bounded-release",
                tempRoot);
            var maximumOpenFileDescriptors = baselineOpenFileDescriptors;
            while (!exportTask.IsCompleted)
            {
                maximumOpenFileDescriptors = Math.Max(
                    maximumOpenFileDescriptors,
                    CountOpenFileDescriptors());
                await Task.Delay(1);
            }

            var result = await exportTask;

            Assert.AreEqual(prefixCount, result.GeneratedPrefixes.Count);
            var additionalOpenFileDescriptors =
                maximumOpenFileDescriptors - baselineOpenFileDescriptors;
            Assert.IsTrue(
                additionalOpenFileDescriptors <= ModuleShardExporter.MaxOpenShardWriters + 16,
                $"A exportação abriu até {additionalOpenFileDescriptors} descritores adicionais; "
                + $"o limite esperado é {ModuleShardExporter.MaxOpenShardWriters} writers "
                + "mais a tolerância dos arquivos do DuckDB.");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, true);
        }
    }

    private static async Task CreateIntegrationParquetAsync(string parquetPath)
    {
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            COPY (
                SELECT *
                FROM (
                    VALUES
                        ('60700007000148', '607', '{{""nome"":""OBRA TESTE""}}', 'hash-1', '2026-04-14T00:00:00Z', '2026-04-14T00:00:00Z'),
                        ('10800000000120', '108', '{{""nome"":""OUTRA OBRA""}}', 'hash-2', '2026-04-14T00:00:00Z', '2026-04-14T00:00:00Z')
                ) AS rows(cnpj, cnpj_prefix, payload_json, content_hash, source_updated_at, module_updated_at)
            )
            TO '{EscapeSqlLiteral(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task CreateManyPrefixesParquetAsync(
        string parquetPath,
        int prefixCount,
        int recordsPerPrefix)
    {
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            COPY (
                SELECT
                    lpad(CAST(prefix AS VARCHAR), 3, '0')
                        || lpad(CAST(record AS VARCHAR), 11, '0') AS cnpj,
                    lpad(CAST(prefix AS VARCHAR), 3, '0') AS cnpj_prefix,
                    '{{""item"":' || CAST(record AS VARCHAR) || '}}' AS payload_json,
                    md5(CAST(prefix AS VARCHAR) || '-' || CAST(record AS VARCHAR)) AS content_hash,
                    '2026-07-24T00:00:00Z' AS source_updated_at,
                    '2026-07-24T00:00:00Z' AS module_updated_at
                FROM range(0, {prefixCount}) prefixes(prefix)
                CROSS JOIN range(0, {recordsPerPrefix}) records(record)
            )
            TO '{EscapeSqlLiteral(parquetPath)}'
            (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";
        await cmd.ExecuteNonQueryAsync();
    }

    private static int CountOpenFileDescriptors() =>
        Directory.EnumerateFileSystemEntries("/proc/self/fd").Count();

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
