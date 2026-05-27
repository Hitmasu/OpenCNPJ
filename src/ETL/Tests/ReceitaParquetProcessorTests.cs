using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.Json;
using ReceitaParquetProcessor = CNPJExporter.Modules.Receita.Processors.ParquetProcessor;

namespace ETL.Tests;

[TestClass]
public sealed class ReceitaParquetProcessorTests
{
    [TestMethod]
    public async Task ConvertCsvsToParquetAsync_ShouldCompactPartitionFilesAfterConversion()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-receita-parquet-{Guid.NewGuid():N}");
        var dataDir = Path.Combine(tempRoot, "data");
        var parquetDir = Path.Combine(tempRoot, "parquet");
        Directory.CreateDirectory(dataDir);

        try
        {
            await File.WriteAllTextAsync(
                Path.Combine(dataDir, "part-0.EMPRECSV"),
                "12300001;RAZAO A;2062;49;100,00;01;\n");
            await File.WriteAllTextAsync(
                Path.Combine(dataDir, "part-1.EMPRECSV"),
                "12300002;RAZAO B;2062;49;200,00;03;\n");

            await using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync();

            var processor = new ReceitaParquetProcessor(dataDir, parquetDir, shardPrefixLength: 3);
            await processor.ConvertCsvsToParquetAsync(connection);

            var partitionDir = Path.Combine(parquetDir, "empresa", "cnpj_prefix=123");
            var parquetFiles = Directory.GetFiles(partitionDir, "*.parquet", SearchOption.TopDirectoryOnly);
            Assert.AreEqual(1, parquetFiles.Length, "A partição deve ser compactada para reduzir opens/scans na geração dos shards.");

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT COUNT(*)
                FROM read_parquet('{parquetDir.Replace("'", "''")}/empresa/**/*.parquet', hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})
                WHERE cnpj_prefix = '123'";
            var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            Assert.AreEqual(2, count);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task ConvertCsvsToParquetAsync_ShouldMaterializeQsaProjection()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-receita-qsa-{Guid.NewGuid():N}");
        var dataDir = Path.Combine(tempRoot, "data");
        var parquetDir = Path.Combine(tempRoot, "parquet");
        Directory.CreateDirectory(dataDir);

        try
        {
            await File.WriteAllTextAsync(
                Path.Combine(dataDir, "SOCIOCSV"),
                string.Join('\n', [
                    "12300001;2;SOCIA A;***000000**;22;20240115;249;12345678901;REP A;05;5",
                    "12300001;2;SOCIO B;***000001**;22;20240216;249;12345678902;REP B;05;6"
                ]) + "\n");
            await File.WriteAllTextAsync(
                Path.Combine(dataDir, "QUALSCSV"),
                string.Join('\n', [
                    "22;Socio",
                    "05;Administrador"
                ]) + "\n");
            await File.WriteAllTextAsync(
                Path.Combine(dataDir, "PAISCSV"),
                "249;ESTADOS UNIDOS\n");

            await using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync();

            var processor = new ReceitaParquetProcessor(dataDir, parquetDir, shardPrefixLength: 3);
            await processor.ConvertCsvsToParquetAsync(connection, qsaMaterializationRangeFanOut: 2);

            var qsaDir = Path.Combine(parquetDir, "qsa", "cnpj_prefix=123");
            Assert.IsTrue(Directory.Exists(qsaDir), "QSA materializado deve ser particionado por cnpj_prefix.");

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT to_json(qsa_data)
                FROM read_parquet('{parquetDir.Replace("'", "''")}/qsa/**/*.parquet', hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})
                WHERE cnpj_prefix = '123' AND cnpj_basico = '12300001'";
            var payload = (await cmd.ExecuteScalarAsync())?.ToString();

            Assert.IsFalse(string.IsNullOrWhiteSpace(payload));
            using var document = JsonDocument.Parse(payload);
            var qsa = document.RootElement;
            Assert.AreEqual(2, qsa.GetArrayLength());
            Assert.AreEqual("Socio", qsa[0].GetProperty("qualificacao_socio").GetString());
            Assert.AreEqual("ESTADOS UNIDOS", qsa[0].GetProperty("pais").GetProperty("descricao").GetString());
            Assert.AreEqual("Administrador", qsa[0].GetProperty("qualificacao_representante").GetProperty("descricao").GetString());
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }
}
