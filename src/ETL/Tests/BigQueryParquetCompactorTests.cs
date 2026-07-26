using CNPJExporter.Processors.BigQuery;
using CNPJExporter.Configuration;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class BigQueryParquetCompactorTests
{
    [TestMethod]
    public void ResolveSettings_ShouldUseBigQueryLimitsInsteadOfPortalImporterLimits()
    {
        var config = new AppConfig
        {
            DuckDb = new AppConfig.DuckDbSettings
            {
                MemoryLimit = "4GB",
                EngineThreads = 1
            },
            PortalTransparenciaIntegration =
                new AppConfig.PortalTransparenciaIntegrationSettings
                {
                    DuckDbMemoryLimit = "512MB",
                    DuckDbThreads = 4,
                    DuckDbMaxTempDirectorySize = "20GB"
                },
            BigQuery = new AppConfig.BigQuerySettings
            {
                CompactionMemoryLimit = "4GB",
                CompactionThreads = 1,
                CompactionMaxTempDirectorySize = "100GB"
            }
        };

        var settings = BigQueryParquetCompactor.ResolveSettings(config);

        Assert.AreEqual("4GB", settings.MemoryLimit);
        Assert.AreEqual(1, settings.Threads);
        Assert.AreEqual("100GB", settings.MaxTempDirectorySize);
    }

    [TestMethod]
    public async Task MaterializeAsync_ShouldCompactGlobIntoOneReusableParquet()
    {
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-bigquery-compact-{Guid.NewGuid():N}");
        var sourceDirectory = Path.Combine(tempRoot, "source");
        Directory.CreateDirectory(sourceDirectory);

        try
        {
            await WritePartAsync(
                Path.Combine(sourceDirectory, "part-001.parquet"),
                "12ABC34501DE35");
            await WritePartAsync(
                Path.Combine(sourceDirectory, "part-002.parquet"),
                "60701190000104");

            var results = await BigQueryParquetCompactor.MaterializeAsync(
                "licitacoes",
                [Path.Combine(sourceDirectory, "*.parquet")],
                tempRoot,
                new BigQueryParquetCompactorSettings(
                    1,
                    "1GB",
                    "2GB"));

            Assert.AreEqual(1, results.Count);
            Assert.IsTrue(File.Exists(results.Single()));
            Assert.IsFalse(results.Single().Contains('*', StringComparison.Ordinal));

            await using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT COUNT(*) FROM read_parquet('{EscapeSqlLiteral(results.Single())}')";
            Assert.AreEqual(2L, Convert.ToInt64(await command.ExecuteScalarAsync()));

            var firstWrite = File.GetLastWriteTimeUtc(results.Single());
            var reused = await BigQueryParquetCompactor.MaterializeAsync(
                "licitacoes",
                [Path.Combine(sourceDirectory, "*.parquet")],
                tempRoot,
                new BigQueryParquetCompactorSettings(
                    1,
                    "1GB",
                    "2GB"));
            Assert.AreEqual(results.Single(), reused.Single());
            Assert.AreEqual(firstWrite, File.GetLastWriteTimeUtc(reused.Single()));
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static async Task WritePartAsync(string path, string cnpj)
    {
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $@"
            COPY (
                SELECT
                    '{cnpj}'::VARCHAR AS cnpj,
                    LEFT('{cnpj}', 3)::VARCHAR AS cnpj_prefix,
                    '{{""updated_at"":""2026-07-25T00:00:00Z""}}'::VARCHAR AS payload_json
            )
            TO '{EscapeSqlLiteral(path)}'
            (FORMAT PARQUET, COMPRESSION ZSTD)";
        await command.ExecuteNonQueryAsync();
    }

    private static string EscapeSqlLiteral(string value) =>
        value.Replace("'", "''", StringComparison.Ordinal);
}
