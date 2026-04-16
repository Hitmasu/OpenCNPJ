using CNPJExporter.Modules.Cno.Models;
using CNPJExporter.Modules.Cno.Processors;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.Json;

namespace ETL.Tests;

[TestClass]
public sealed class ParquetProcessorTests
{
    [TestMethod]
    public async Task ConvertToParquetAsync_ShouldIndexCnpjs_FromObrasAndVinculos()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-cno-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var files = new ExtractedFiles(
                WriteCsv(tempRoot, "cno.csv", 26, [
                    "010010092278,105,BRASIL,1992-02-20,1992-02-20,2022-05-17,,71215207,02688984000170,0057,Obra A,9701,Brasilia,OUTROS,SOF SUL,SN,Bairro,DF,,Lote,m2,412.00,02,2002-11-30,Empresa A,"
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
            var parquetPath = Path.Combine(tempRoot, "cno.parquet");
            var processor = new ParquetProcessor();

            await processor.ConvertToParquetAsync(
                files,
                parquetPath,
                new DateTimeOffset(2026, 4, 14, 12, 0, 0, TimeSpan.Zero),
                shardPrefixLength: 3);
            var hashes = await processor.LoadHashesAsync(parquetPath);

            Assert.IsTrue(hashes.ContainsKey("02688984000170"), "CNPJ do responsável da obra deve ser indexado.");
            Assert.IsTrue(hashes.ContainsKey("12345678000195"), "CNPJ vinculado à obra deve ser indexado.");
            Assert.AreEqual(2, hashes.Count);

            var payload = await ReadPayloadAsync(parquetPath, "02688984000170");
            using var document = JsonDocument.Parse(payload);
            var obra = document.RootElement.GetProperty("obras")[0];
            Assert.AreEqual("02", obra.GetProperty("situacao").GetProperty("codigo").GetString());
            Assert.AreEqual("ATIVA", obra.GetProperty("situacao").GetProperty("descricao").GetString());
            Assert.AreEqual("0057", obra.GetProperty("qualificacao_responsavel").GetProperty("codigo").GetString());
            Assert.AreEqual("Dono da Obra", obra.GetProperty("qualificacao_responsavel").GetProperty("descricao").GetString());
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static string WriteCsv(string directory, string fileName, int columnCount, IReadOnlyList<string> rows)
    {
        var path = Path.Combine(directory, fileName);
        var header = string.Join(",", Enumerable.Range(1, columnCount).Select(static index => $"col{index}"));
        File.WriteAllLines(path, [header, .. rows]);
        return path;
    }

    private static async Task<string> ReadPayloadAsync(string parquetPath, string cnpj)
    {
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT payload_json
            FROM read_parquet('{parquetPath.Replace("'", "''")}')
            WHERE cnpj = '{cnpj.Replace("'", "''")}'";
        var result = await cmd.ExecuteScalarAsync();
        return result?.ToString() ?? throw new AssertFailedException($"Payload não encontrado para {cnpj}.");
    }
}
