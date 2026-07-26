using System.Text;
using System.Text.Json;
using CNPJExporter.Modules.PortalTransparencia.Configuration;
using CNPJExporter.Modules.PortalTransparencia.Models;
using CNPJExporter.Modules.PortalTransparencia.Processors;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class PortalTransparenciaParquetProcessorTests
{
    private static readonly DateTimeOffset UpdatedAt =
        new(2026, 7, 23, 16, 22, 32, TimeSpan.Zero);

    [TestMethod]
    public async Task FavorecidosPj_ShouldPublishOnlyCnpjAndJoinDictionaries()
    {
        var tempRoot = CreateTempRoot();

        try
        {
            var companies = WriteWindows1252Csv(
                tempRoot,
                "202606_CNPJ.csv",
                [
                    [
                        "12.abc.345/01de-35",
                        "Empresa Ágil Ltda",
                        "Ágil",
                        "6201501",
                        "2062",
                        "J",
                        "Rua São João",
                        "100",
                        "Sala 1",
                        "01001000",
                        "Centro",
                        "São Paulo",
                        "SP"
                    ],
                    [
                        "123.456.789-00",
                        "Pessoa física",
                        "",
                        "",
                        "",
                        "F",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        ""
                    ]
                ]);
            var cnaes = WriteWindows1252Csv(
                tempRoot,
                "202606_CNAE.csv",
                [["J", "Informação e comunicação", "6201501", "Desenvolvimento de programas"]]);
            var legalNatures = WriteWindows1252Csv(
                tempRoot,
                "202606_NaturezaJuridica.csv",
                [["2062", "Sociedade Empresária Limitada", "2", "Entidades empresariais"]]);

            var (parquetPath, hashes) = await ProcessAsync(
                "favorecidos_pj",
                tempRoot,
                [companies, cnaes, legalNatures]);

            CollectionAssert.AreEquivalent(
                new[] { "12ABC34501DE35" },
                hashes.Keys.ToArray());
            using var payload = JsonDocument.Parse(
                await ReadPayloadAsync(parquetPath, "12ABC34501DE35"));
            Assert.AreEqual(
                "Empresa Ágil Ltda",
                payload.RootElement.GetProperty("razao_social").GetString());
            Assert.AreEqual(
                "Desenvolvimento de programas",
                payload.RootElement
                    .GetProperty("cnae")
                    .GetProperty("descricao")
                    .GetString());
            Assert.AreEqual(
                "São Paulo",
                payload.RootElement
                    .GetProperty("endereco")
                    .GetProperty("municipio")
                    .GetString());
        }
        finally
        {
            DeleteTempRoot(tempRoot);
        }
    }

    [TestMethod]
    public async Task FavorecidosPj_ShouldFailWhenSourceContainsDuplicateCnpj()
    {
        var tempRoot = CreateTempRoot();

        try
        {
            var duplicatedRow = new[]
            {
                "11.193.322/0001-10",
                "Empresa Duplicada",
                "",
                "6201501",
                "2062",
                "J",
                "",
                "",
                "",
                "",
                "",
                "",
                ""
            };
            var companies = WriteWindows1252Csv(
                tempRoot,
                "202606_CNPJ.csv",
                [duplicatedRow, duplicatedRow]);
            var cnaes = WriteWindows1252Csv(
                tempRoot,
                "202606_CNAE.csv",
                [["J", "Informação", "6201501", "Desenvolvimento"]]);
            var legalNatures = WriteWindows1252Csv(
                tempRoot,
                "202606_NaturezaJuridica.csv",
                [["2062", "Limitada", "2", "Empresarial"]]);

            var exception = await Assert.ThrowsExceptionAsync<InvalidDataException>(
                () => ProcessAsync(
                    "favorecidos_pj",
                    tempRoot,
                    [companies, cnaes, legalNatures]));

            StringAssert.Contains(exception.Message, "mais de uma linha");
        }
        finally
        {
            DeleteTempRoot(tempRoot);
        }
    }

    [TestMethod]
    public async Task Ceis_ShouldGroupSanctionsAndExcludeCpfRows()
    {
        var tempRoot = CreateTempRoot();

        try
        {
            var csv = WriteWindows1252Csv(
                tempRoot,
                "20260723_CEIS.csv",
                [
                    CeisRow("S2", "11.193.322/0001-10", "Empresa Ágil", "Processo 2"),
                    CeisRow("S1", "11.193.322/0001-10", "Empresa Ágil", "Processo 1"),
                    CeisRow("PF1", "123.456.789-00", "Pessoa física", "Processo PF", "Pessoa Física")
                ]);

            var (parquetPath, hashes) = await ProcessAsync(
                "ceis",
                tempRoot,
                [csv]);

            CollectionAssert.AreEquivalent(
                new[] { "11193322000110" },
                hashes.Keys.ToArray());
            using var payload = JsonDocument.Parse(
                await ReadPayloadAsync(parquetPath, "11193322000110"));
            var sanctions = payload.RootElement.GetProperty("sancoes");
            Assert.AreEqual(2, sanctions.GetArrayLength());
            Assert.AreEqual("S1", sanctions[0].GetProperty("codigo").GetString());
            Assert.AreEqual(
                "Empresa Ágil",
                sanctions[0].GetProperty("nome_sancionado").GetString());
        }
        finally
        {
            DeleteTempRoot(tempRoot);
        }
    }

    [TestMethod]
    public async Task Cepim_ShouldGroupImpedimentsByCnpj()
    {
        var tempRoot = CreateTempRoot();

        try
        {
            var csv = WriteWindows1252Csv(
                tempRoot,
                "20260722_CEPIM.csv",
                [
                    ["60.452.651/0001-44", "Instituto Ágil", "002", "Órgão B", "Motivo B"],
                    ["60.452.651/0001-44", "Instituto Ágil", "001", "Órgão A", "Motivo A"],
                    ["123.456.789-00", "Pessoa física", "003", "Órgão C", "Motivo C"]
                ]);

            var (parquetPath, hashes) = await ProcessAsync(
                "cepim",
                tempRoot,
                [csv]);

            CollectionAssert.AreEquivalent(
                new[] { "60452651000144" },
                hashes.Keys.ToArray());
            using var payload = JsonDocument.Parse(
                await ReadPayloadAsync(parquetPath, "60452651000144"));
            var impediments = payload.RootElement.GetProperty("impedimentos");
            Assert.AreEqual(2, impediments.GetArrayLength());
            Assert.AreEqual(
                "001",
                impediments[0].GetProperty("numero_convenio").GetString());
        }
        finally
        {
            DeleteTempRoot(tempRoot);
        }
    }

    [TestMethod]
    public async Task Cnep_ShouldPreserveFineAndExcludeCpfRows()
    {
        var tempRoot = CreateTempRoot();

        try
        {
            var csv = WriteWindows1252Csv(
                tempRoot,
                "20260723_CNEP.csv",
                [
                    CnepRow("CNEP-1", "60.452.651/0001-44", "Empresa Punida", "1.234,56"),
                    CnepRow(
                        "PF-1",
                        "123.456.789-00",
                        "Pessoa física",
                        "10,00",
                        "Pessoa Física")
                ]);

            var (parquetPath, hashes) = await ProcessAsync(
                "cnep",
                tempRoot,
                [csv]);

            CollectionAssert.AreEquivalent(
                new[] { "60452651000144" },
                hashes.Keys.ToArray());
            using var payload = JsonDocument.Parse(
                await ReadPayloadAsync(parquetPath, "60452651000144"));
            Assert.AreEqual(
                "1.234,56",
                payload.RootElement
                    .GetProperty("sancoes")[0]
                    .GetProperty("valor_multa")
                    .GetString());
        }
        finally
        {
            DeleteTempRoot(tempRoot);
        }
    }

    [TestMethod]
    public async Task AcordosLeniencia_ShouldJoinEffectsAndExcludeNonCnpjRows()
    {
        var tempRoot = CreateTempRoot();

        try
        {
            var agreements = WriteWindows1252Csv(
                tempRoot,
                "20260723_Acordos.csv",
                [
                    [
                        "A1",
                        "11.193.322/0001-10",
                        "Empresa Ágil",
                        "Ágil",
                        "01/01/2025",
                        "01/01/2027",
                        "Vigente",
                        "23/07/2026",
                        "Processo 1",
                        "Termos públicos",
                        "CGU"
                    ],
                    [
                        "A2",
                        "123.456.789-00",
                        "Pessoa física",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        ""
                    ]
                ]);
            var effects = WriteWindows1252Csv(
                tempRoot,
                "20260723_Efeitos.csv",
                [
                    ["A1", "Efeito B", "Complemento B"],
                    ["A1", "Efeito A", "Complemento A"],
                    ["A2", "Efeito PF", ""]
                ]);

            var (parquetPath, hashes) = await ProcessAsync(
                "acordos_leniencia",
                tempRoot,
                [agreements, effects]);

            CollectionAssert.AreEquivalent(
                new[] { "11193322000110" },
                hashes.Keys.ToArray());
            using var payload = JsonDocument.Parse(
                await ReadPayloadAsync(parquetPath, "11193322000110"));
            var agreement = payload.RootElement.GetProperty("acordos")[0];
            Assert.AreEqual("A1", agreement.GetProperty("id").GetString());
            Assert.AreEqual(2, agreement.GetProperty("efeitos").GetArrayLength());
            Assert.AreEqual(
                "Efeito A",
                agreement.GetProperty("efeitos")[0].GetProperty("efeito").GetString());
        }
        finally
        {
            DeleteTempRoot(tempRoot);
        }
    }

    private static string[] CeisRow(
        string code,
        string document,
        string name,
        string process,
        string personType = "Pessoa Jurídica") =>
    [
        "CEIS",
        code,
        personType,
        document,
        name,
        name,
        name,
        "Fantasia",
        process,
        "Impedimento",
        "01/01/2025",
        "01/01/2027",
        "02/01/2025",
        "DOU",
        "Seção 1",
        "",
        "Nacional",
        "CGU",
        "DF",
        "Federal",
        "Lei",
        "23/07/2026",
        "CGU",
        "Observação"
    ];

    private static string[] CnepRow(
        string code,
        string document,
        string name,
        string fine,
        string personType = "Pessoa Jurídica") =>
    [
        "CNEP",
        code,
        personType,
        document,
        name,
        name,
        name,
        "Fantasia",
        $"Processo {code}",
        "Multa",
        fine,
        "01/01/2025",
        "01/01/2027",
        "02/01/2025",
        "DOU",
        "Seção 1",
        "",
        "Nacional",
        "CGU",
        "DF",
        "Federal",
        "Lei",
        "23/07/2026",
        "CGU",
        "Observação"
    ];

    private static async Task<(string ParquetPath, Dictionary<string, string> Hashes)> ProcessAsync(
        string datasetKey,
        string tempRoot,
        IReadOnlyList<string> csvPaths)
    {
        var options = new IntegrationOptions
        {
            DuckDbThreads = 1,
            DuckDbMemoryLimit = "128MB",
            DuckDbMaxTempDirectorySize = "1GB"
        };
        var processor = new ParquetProcessor(options);
        var parquetPath = Path.Combine(tempRoot, $"{datasetKey}.parquet");
        await processor.ConvertToParquetAsync(
            PortalDatasetDefinition.GetRequired(datasetKey),
            new ExtractedDataset(csvPaths),
            parquetPath,
            UpdatedAt,
            shardPrefixLength: 3);

        return (parquetPath, await processor.LoadHashesAsync(parquetPath));
    }

    private static string WriteWindows1252Csv(
        string directory,
        string fileName,
        IReadOnlyList<string[]> rows)
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var columnCount = rows.First().Length;
        Assert.IsTrue(
            rows.All(row => row.Length == columnCount),
            $"Todas as linhas de {fileName} devem ter {columnCount} colunas.");
        var header = Enumerable
            .Range(1, columnCount)
            .Select(index => $"COLUNA {index}")
            .ToArray();
        var contentRows = new[] { header }
            .Concat(rows)
            .Select(row => string.Join(';', row.Select(EscapeCsvValue)));
        var path = Path.Combine(directory, fileName);
        File.WriteAllText(
            path,
            string.Join("\r\n", contentRows),
            Encoding.GetEncoding(1252));
        return path;
    }

    private static string EscapeCsvValue(string value) =>
        $"\"{value.Replace("\"", "\"\"")}\"";

    private static async Task<string> ReadPayloadAsync(string parquetPath, string cnpj)
    {
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $@"
            SELECT payload_json
            FROM read_parquet('{parquetPath.Replace("'", "''")}')
            WHERE cnpj = '{cnpj.Replace("'", "''")}'";
        var result = await command.ExecuteScalarAsync();
        return result?.ToString()
               ?? throw new AssertFailedException($"Payload não encontrado para {cnpj}.");
    }

    private static string CreateTempRoot()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-portal-processor-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempRoot(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
