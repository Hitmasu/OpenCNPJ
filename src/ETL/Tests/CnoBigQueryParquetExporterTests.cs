using System.Text.Json;
using CNPJExporter.Modules.Cno.Models;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CnoBigQueryParquetExporter = CNPJExporter.Modules.Cno.BigQueryParquetExporter;
using CnoParquetProcessor = CNPJExporter.Modules.Cno.Processors.ParquetProcessor;

namespace ETL.Tests;

[TestClass]
public sealed class CnoBigQueryParquetExporterTests
{
    [TestMethod]
    public async Task MaterializeAsync_ShouldCreateColumnarParquetWithoutPayloadJson()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-cno-bigquery-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var files = new ExtractedFiles(
                WriteCsv(tempRoot, "cno.csv", 26, [
                    "010010092278,105,BRASIL,1992-02-20,1992-02-21,2022-05-17,,71215207,12.abc.345/01de-35,0057,Obra A,9701,Brasilia,OUTROS,SOF SUL,SN,Bairro,DF,,Lote,m2,412.00,02,2002-11-30,Empresa A,001"
                ]),
                WriteCsv(tempRoot, "cno_cnaes.csv", 3, [
                    "010010092278,4120400,2022-05-17"
                ]),
                WriteCsv(tempRoot, "cno_vinculos.csv", 6, [
                    "010010092278,2022-05-17,,2022-05-17,0053,12.345.678/0001-95"
                ]),
                WriteCsv(tempRoot, "cno_areas.csv", 7, [
                    "010010092278,1,Residencial,Construcao,Principal,,412.00"
                ]));
            var canonicalParquetPath = Path.Combine(tempRoot, "cno.parquet");

            await new CnoParquetProcessor().ConvertToParquetAsync(
                files,
                canonicalParquetPath,
                new DateTimeOffset(2026, 4, 14, 12, 0, 0, TimeSpan.Zero),
                shardPrefixLength: 3);

            var source = await new CnoBigQueryParquetExporter(canonicalParquetPath).MaterializeAsync();

            Assert.AreEqual("cno", source.TableName);
            Assert.AreEqual(1, source.SourcePaths.Count);
            Assert.IsTrue(File.Exists(source.SourcePaths.Single()));

            await using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync();

            var columnNames = await ReadColumnNamesAsync(connection, source.SourcePaths.Single());
            Assert.IsFalse(columnNames.Contains("payload_json"), "BigQuery CNO não deve carregar payload_json.");
            CollectionAssert.IsSubsetOf(
                new[] { "cnpj", "cnpj_prefix", "updated_at", "obras" },
                columnNames.ToArray());

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT cnpj, cnpj_prefix, updated_at, to_json(obras) AS obras_json
                FROM read_parquet('{EscapeSqlLiteral(source.SourcePaths.Single())}')
                WHERE cnpj = '12ABC34501DE35'";
            await using var reader = await cmd.ExecuteReaderAsync();

            Assert.IsTrue(await reader.ReadAsync());
            Assert.AreEqual("12ABC34501DE35", reader.GetString(0));
            Assert.AreEqual("12A", reader.GetString(1));
            Assert.AreEqual("2026-04-14T12:00:00.0000000+00:00", reader.GetString(2));

            using var obras = JsonDocument.Parse(reader.GetString(3));
            Assert.AreEqual(1, obras.RootElement.GetArrayLength());
            var obra = obras.RootElement[0];
            Assert.AreEqual("010010092278", obra.GetProperty("cno").GetString());
            Assert.AreEqual("ATIVA", obra.GetProperty("situacao").GetProperty("descricao").GetString());
            Assert.AreEqual("0057", obra.GetProperty("qualificacao_responsavel").GetProperty("codigo").GetString());
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static async Task<HashSet<string>> ReadColumnNamesAsync(DuckDBConnection connection, string parquetPath)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT * FROM read_parquet('{EscapeSqlLiteral(parquetPath)}') LIMIT 0";
        await using var reader = await cmd.ExecuteReaderAsync();

        return Enumerable
            .Range(0, reader.FieldCount)
            .Select(reader.GetName)
            .ToHashSet(StringComparer.Ordinal);
    }

    private static string WriteCsv(string directory, string fileName, int columnCount, IReadOnlyList<string> rows)
    {
        var path = Path.Combine(directory, fileName);
        var header = string.Join(",", Enumerable.Range(1, columnCount).Select(static index => $"col{index}"));
        File.WriteAllLines(path, [header, .. rows]);
        return path;
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
