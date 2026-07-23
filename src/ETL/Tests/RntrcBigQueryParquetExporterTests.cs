using System.Text;
using CNPJExporter.Modules.Rntrc.Processors;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RntrcBigQueryParquetExporter = CNPJExporter.Modules.Rntrc.BigQueryParquetExporter;

namespace ETL.Tests;

[TestClass]
public sealed class RntrcBigQueryParquetExporterTests
{
    [TestMethod]
    public async Task MaterializeAsync_ShouldCreateColumnarParquetWithoutPayloadJson()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-rntrc-bigquery-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var csvPath = WriteLatin1Csv(tempRoot, "transportadores_rntrc_03_2026.csv", [
                "\"Transportadora Ágil LTDA\";\"050085788\";\"23/05/2017\";\"ATIVO\";\"12.abc.345/01de-35\";\"ETC\";\"14095-290\";\"RIBEIRÃO PRETO\";\"SP\";\"Sim\";\"23/10/2024\"",
                "\"+ RAPIDO TRANSPORTADORA LTDA\";\"058308655\";\"23/07/2025\";\"PENDENTE\";\"60.452.651/0001-44\";\"ETC\";\"18120-000\";\"MAIRINQUE\";\"SP\";\"Não\";\"23/07/2025\""
            ]);
            var canonicalParquetPath = Path.Combine(tempRoot, "rntrc.parquet");

            await new ParquetProcessor().ConvertToParquetAsync(
                csvPath,
                canonicalParquetPath,
                new DateTimeOffset(2026, 4, 10, 9, 9, 56, TimeSpan.Zero),
                shardPrefixLength: 3);

            var source = await new RntrcBigQueryParquetExporter(canonicalParquetPath).MaterializeAsync();

            Assert.AreEqual("rntrc", source.TableName);
            Assert.AreEqual(1, source.SourcePaths.Count);
            Assert.IsTrue(File.Exists(source.SourcePaths.Single()));

            await using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync();

            var columnNames = await ReadColumnNamesAsync(connection, source.SourcePaths.Single());
            Assert.IsFalse(columnNames.Contains("payload_json"), "BigQuery RNTRC não deve carregar payload_json.");
            CollectionAssert.IsSubsetOf(
                new[]
                {
                    "cnpj",
                    "cnpj_prefix",
                    "updated_at",
                    "numero_rntrc",
                    "nome",
                    "categoria",
                    "situacao",
                    "data_primeiro_cadastro",
                    "data_situacao",
                    "cep",
                    "municipio",
                    "uf",
                    "equiparado"
                },
                columnNames.ToArray());

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT
                    cnpj,
                    cnpj_prefix,
                    updated_at,
                    numero_rntrc,
                    nome,
                    municipio,
                    equiparado
                FROM read_parquet('{EscapeSqlLiteral(source.SourcePaths.Single())}')
                WHERE cnpj = '12ABC34501DE35'";
            await using var reader = await cmd.ExecuteReaderAsync();

            Assert.IsTrue(await reader.ReadAsync());
            Assert.AreEqual("12ABC34501DE35", reader.GetString(0));
            Assert.AreEqual("12A", reader.GetString(1));
            Assert.AreEqual("2026-04-10T09:09:56.0000000+00:00", reader.GetString(2));
            Assert.AreEqual("050085788", reader.GetString(3));
            Assert.AreEqual("Transportadora Ágil LTDA", reader.GetString(4));
            Assert.AreEqual("RIBEIRÃO PRETO", reader.GetString(5));
            Assert.IsTrue(reader.GetBoolean(6));
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

    private static string WriteLatin1Csv(string directory, string fileName, IReadOnlyList<string> rows)
    {
        var path = Path.Combine(directory, fileName);
        var header = "nome_transportador;numero_rntrc;data_primeiro_cadastro;situacao_rntrc;cpfcnpjtransportador;categoria_transportador;cep;municipio;uf;equiparado;data_situacao_rntrc";
        File.WriteAllText(path, string.Join("\r\n", [header, .. rows]), Encoding.Latin1);
        return path;
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
