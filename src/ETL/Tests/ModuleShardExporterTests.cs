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
                tempRoot,
                ["607"]);

            var dataPath = Path.Combine(result.LocalShardDir, "607.ndjson");
            var indexPath = Path.Combine(result.LocalShardDir, "607.index.bin");

            Assert.IsTrue(File.Exists(dataPath), "O shard sparse do módulo deveria ser gerado.");
            Assert.IsTrue(File.Exists(indexPath), "O índice binário do módulo deveria ser gerado.");
            CollectionAssert.AreEqual(new[] { "607" }, result.GeneratedPrefixes.ToArray());

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

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
