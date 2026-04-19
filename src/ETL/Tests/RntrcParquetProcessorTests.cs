using System.Text;
using System.Text.Json;
using CNPJExporter.Modules.Rntrc.Processors;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class RntrcParquetProcessorTests
{
    [TestMethod]
    public async Task ConvertToParquetAsync_ShouldIndexOnlyCnpjs_FromLatin1SemicolonCsv()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-rntrc-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var csvPath = WriteLatin1Csv(tempRoot, "transportadores_rntrc_03_2026.csv", [
                "\"Transportadora Ágil LTDA\";\"050085788\";\"23/05/2017\";\"ATIVO\";\"11.193.322/0001-10\";\"ETC\";\"14095-290\";\"RIBEIRÃO PRETO\";\"SP\";\"Sim\";\"23/10/2024\"",
                "\"Autônomo Anonimizado\";\"055515644\";\"07/12/2022\";\"ATIVO\";\"***.123.456-**\";\"TAC\";\"14080-080\";\"RIBEIRÃO PRETO\";\"SP\";\"Não\";\"23/07/2025\"",
                "\"+ RAPIDO TRANSPORTADORA LTDA\";\"058308655\";\"23/07/2025\";\"PENDENTE\";\"60.452.651/0001-44\";\"ETC\";\"18120-000\";\"MAIRINQUE\";\"SP\";\"Não\";\"23/07/2025\""
            ]);
            var parquetPath = Path.Combine(tempRoot, "rntrc.parquet");
            var processor = new ParquetProcessor();

            await processor.ConvertToParquetAsync(
                csvPath,
                parquetPath,
                new DateTimeOffset(2026, 4, 10, 9, 9, 56, TimeSpan.Zero),
                shardPrefixLength: 3);
            var hashes = await processor.LoadHashesAsync(parquetPath);

            Assert.IsTrue(hashes.ContainsKey("11193322000110"), "CNPJ formatado deve ser normalizado e indexado.");
            Assert.IsTrue(hashes.ContainsKey("60452651000144"), "Segundo CNPJ formatado deve ser normalizado e indexado.");
            Assert.AreEqual(2, hashes.Count, "CPF/TAC anonimizado não deve entrar no índice CNPJ.");

            var payload = await ReadPayloadAsync(parquetPath, "11193322000110");
            using var document = JsonDocument.Parse(payload);
            Assert.IsFalse(
                document.RootElement.TryGetProperty("transportadores", out _),
                "RNTRC deve expor um único transportador por CNPJ, sem array.");
            Assert.AreEqual("Transportadora Ágil LTDA", document.RootElement.GetProperty("nome").GetString());
            Assert.AreEqual("RIBEIRÃO PRETO", document.RootElement.GetProperty("municipio").GetString());
            Assert.AreEqual("ETC", document.RootElement.GetProperty("categoria").GetString());
            Assert.IsTrue(document.RootElement.GetProperty("equiparado").GetBoolean());
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static string WriteLatin1Csv(string directory, string fileName, IReadOnlyList<string> rows)
    {
        var path = Path.Combine(directory, fileName);
        var header = "nome_transportador;numero_rntrc;data_primeiro_cadastro;situacao_rntrc;cpfcnpjtransportador;categoria_transportador;cep;municipio;uf;equiparado;data_situacao_rntrc";
        File.WriteAllText(path, string.Join("\r\n", [header, .. rows]), Encoding.Latin1);
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
