using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using CNPJExporter.Modules.PortalTransparencia.Configuration;
using CNPJExporter.Modules.PortalTransparencia.Models;
using CNPJExporter.Modules.PortalTransparencia.Processors;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class PortalTransparenciaPartitionedProcessorTests
{
    private const string AlphaCnpj = "12ABC34501DE35";
    private const string NumericCnpj = "11193322000110";
    private static readonly DateTimeOffset UpdatedAt =
        new(2026, 7, 23, 16, 22, 32, TimeSpan.Zero);

    [TestMethod]
    public async Task Licitacoes_ShouldPublishEveryRelatedRecordForAlphaNumericCnpj()
    {
        var root = CreateTempRoot();
        try
        {
            var files = new[]
            {
                WriteCsv(root, "202601_Licitação.csv", 17,
                    Row(17, (0, "L1"), (1, "UG1"), (2, "Unidade"), (3, "M1"),
                        (4, "Pregão"), (5, "P1"), (6, "Objeto"), (7, "Concluída"),
                        (16, "1.000,00"))),
                WriteCsv(root, "202601_ItemLicitação.csv", 14,
                    Row(14, (0, "L1"), (1, "UG1"), (3, "M1"), (5, "P1"),
                        (8, "I1"), (9, "Serviço"), (10, "1"), (11, "100,00"),
                        (12, AlphaCnpj), (13, "Empresa vencedora"))),
                WriteCsv(root, "202601_ParticipantesLicitação.csv", 13,
                    Row(13, (0, "L1"), (1, "UG1"), (3, "M1"), (5, "P1"),
                        (8, "I1"), (9, "Serviço"), (10, AlphaCnpj),
                        (11, "Empresa participante"), (12, "SIM")),
                    Row(13, (0, "L1"), (1, "UG1"), (3, "M1"), (5, "P1"),
                        (8, "I1"), (10, "12345678900"), (11, "Pessoa"))),
                WriteCsv(root, "202601_EmpenhosRelacionados.csv", 10,
                    Row(10, (0, "L1"), (1, "UG1"), (3, "M1"), (5, "P1"),
                        (6, "E1"), (7, "20/01/2026"), (8, "Empenho"),
                        (9, "100,00")))
            };

            using var payload = await ProcessAndReadAsync(
                "licitacoes",
                root,
                files,
                AlphaCnpj);

            var records = payload.RootElement.GetProperty("licitacoes");
            CollectionAssert.AreEquivalent(
                new[] { "licitacao", "participacao", "item_vencido", "empenho" },
                records.EnumerateArray()
                    .Select(record => record.GetProperty("tipo_registro").GetString())
                    .ToArray());
            Assert.AreEqual("2026-07-23T16:22:32.0000000+00:00",
                payload.RootElement.GetProperty("updated_at").GetString());
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    [TestMethod]
    public async Task Contratos_ShouldLinkItemsTermsAndApostilamentosToContractor()
    {
        var root = CreateTempRoot();
        try
        {
            var files = new[]
            {
                WriteCsv(root, "202601_Compras.csv", 24,
                    Row(24, (0, "C1"), (1, "Objeto"), (7, "O1"), (9, "UG1"),
                        (15, NumericCnpj), (16, "Contratada"), (17, "10,00"),
                        (18, "12,00"))),
                WriteCsv(root, "202601_ItemCompra.csv", 10,
                    Row(10, (0, "O1"), (2, "UG1"), (4, "C1"), (5, "I1"),
                        (6, "Item"), (8, "2"), (9, "5,00"))),
                WriteCsv(root, "202601_TermoAditivo.csv", 10,
                    Row(10, (0, "C1"), (3, "O1"), (5, "UG1"), (7, "T1"),
                        (8, "01/02/2026"), (9, "Prorrogação"))),
                WriteCsv(root, "202601_Apostilamento.csv", 12,
                    Row(12, (0, "C1"), (3, "O1"), (5, "UG1"), (7, "A1"),
                        (8, "Reajuste"), (9, "2,00")))
            };

            using var payload = await ProcessAndReadAsync(
                "contratos",
                root,
                files,
                NumericCnpj);

            CollectionAssert.AreEquivalent(
                new[] { "contrato", "item", "termo_aditivo", "apostilamento" },
                ReadTypes(payload, "contratos"));
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    [TestMethod]
    public async Task Renuncias_ShouldUnifyTheFourOfficialFiles()
    {
        var root = CreateTempRoot();
        try
        {
            var files = new[]
            {
                WriteCsv(root, "2024_RenúnciasFiscais.csv", 15,
                    Row(15, (0, "2024"), (1, NumericCnpj), (2, "Empresa"),
                        (8, "Incentivo"), (9, "Benefício"), (12, "IR"),
                        (14, "100,00"))),
                WriteCsv(root, "2024_EmpresasHabilitadas.csv", 12,
                    Row(12, (0, NumericCnpj), (1, "Empresa"), (7, "Benefício"),
                        (10, "01/01/2024"))),
                WriteCsv(root, "2024_EmpresasImunesOuIsentas.csv", 10,
                    Row(10, (0, "2024"), (1, NumericCnpj), (2, "Empresa"),
                        (8, "Entidade"), (9, "Imunidade"))),
                WriteCsv(root, "2024_RenúnciasFiscaisPorBeneficiário.csv", 9,
                    Row(9, (0, "2024"), (1, NumericCnpj), (2, "Empresa"),
                        (8, "100,00")))
            };

            using var payload = await ProcessAndReadAsync(
                "renuncias_fiscais",
                root,
                files,
                NumericCnpj);

            CollectionAssert.AreEquivalent(
                new[]
                {
                    "renuncia_fiscal",
                    "empresa_habilitada",
                    "empresa_imune_ou_isenta",
                    "total_por_beneficiario"
                },
                ReadTypes(payload, "renuncias"));
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    [TestMethod]
    public async Task NotasFiscais_ShouldIndexIssuerAndRecipientWithItemsAndEvents()
    {
        var root = CreateTempRoot();
        try
        {
            var files = new[]
            {
                WriteCsv(root, "202601_NFe_NotaFiscal.csv", 25,
                    Row(25, (0, "NFE1"), (3, "10"), (5, "10/01/2026"),
                        (8, AlphaCnpj), (9, "Emitente"), (17, NumericCnpj),
                        (18, "Destinatário"), (24, "50,00"))),
                WriteCsv(root, "202601_NFe_NotaFiscalItem.csv", 31,
                    Row(31, (0, "NFE1"), (6, AlphaCnpj),
                        (15, NumericCnpj), (22, "1"), (23, "Produto"),
                        (27, "2"), (28, "UN"), (29, "25,00"), (30, "50,00"))),
                WriteCsv(root, "202601_NFe_NotaFiscalEvento.csv", 10,
                    Row(10, (0, "NFE1"), (6, "Autorização"),
                        (7, "10/01/2026"), (8, "Autorizada")))
            };

            using var issuer = await ProcessAndReadAsync(
                "notas_fiscais",
                root,
                files,
                AlphaCnpj);
            CollectionAssert.AreEquivalent(
                new[] { "nota_fiscal", "item", "evento" },
                ReadTypes(issuer, "notas_fiscais"));
            Assert.IsTrue(
                issuer.RootElement.GetProperty("notas_fiscais")
                    .EnumerateArray()
                    .All(record =>
                        record.GetProperty("papel_cnpj").GetString() == "emitente"));

            using var recipient = await ReadPayloadAsync(
                Path.Combine(root, "output", "*.parquet"),
                NumericCnpj);
            Assert.IsTrue(
                recipient.RootElement.GetProperty("notas_fiscais")
                    .EnumerateArray()
                    .All(record =>
                        record.GetProperty("papel_cnpj").GetString() == "destinatario"));
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    [TestMethod]
    public async Task NotasFiscais_ShouldProcessMonthlyArchivesAsIndependentBatches()
    {
        var root = CreateTempRoot();
        try
        {
            var files = new List<string>();
            foreach (var (directoryName, key, date) in new[]
                     {
                         ("202601", "NFE1", "10/01/2026"),
                         ("202602", "NFE2", "10/02/2026")
                     })
            {
                var directory = Path.Combine(root, directoryName);
                Directory.CreateDirectory(directory);
                files.Add(WriteCsv(directory, $"{directoryName}_NFe_NotaFiscal.csv", 25,
                    Row(25, (0, key), (5, date), (8, AlphaCnpj),
                        (9, "Emitente"), (17, NumericCnpj),
                        (18, "Destinatário"), (24, "50,00"))));
                files.Add(WriteCsv(directory, $"{directoryName}_NFe_NotaFiscalItem.csv", 31,
                    Row(31, (0, key), (6, AlphaCnpj), (15, NumericCnpj),
                        (22, "1"), (23, "Produto"), (27, "1"),
                        (28, "UN"), (29, "50,00"), (30, "50,00"))));
                files.Add(WriteCsv(directory, $"{directoryName}_NFe_NotaFiscalEvento.csv", 10,
                    Row(10, (0, key), (6, "Autorização"), (7, date),
                        (8, "Autorizada"))));
            }

            using var payload = await ProcessAndReadAsync(
                "notas_fiscais",
                root,
                files,
                AlphaCnpj);
            CollectionAssert.AreEquivalent(
                new[] { "NFE1", "NFE2" },
                payload.RootElement.GetProperty("notas_fiscais")
                    .EnumerateArray()
                    .Where(record =>
                        record.GetProperty("tipo_registro").GetString()
                        == "nota_fiscal")
                    .Select(record =>
                        record.GetProperty("chave_acesso").GetString())
                    .ToArray());
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    [TestMethod]
    public async Task Convenios_ShouldLinkBankOrdersToConvenente()
    {
        var root = CreateTempRoot();
        try
        {
            var files = new[]
            {
                WriteCsv(root, "20260723_Convenios.csv", 27,
                    Row(27, (0, "V1"), (7, "Objeto"), (14, NumericCnpj),
                        (16, "Convenente"), (19, "100,00"))),
                WriteCsv(root, "20260723_Convenios_OrdensBancarias.csv", 5,
                    Row(5, (0, "V1"), (2, "20/07/2026"), (3, "OB1"),
                        (4, "50,00")))
            };

            using var payload = await ProcessAndReadAsync(
                "convenios",
                root,
                files,
                NumericCnpj);
            CollectionAssert.AreEquivalent(
                new[] { "convenio", "ordem_bancaria" },
                ReadTypes(payload, "convenios"));
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    [TestMethod]
    public async Task Emendas_ShouldKeepOnlyDirectCnpjFavorecimentos()
    {
        var root = CreateTempRoot();
        try
        {
            var files = new[]
            {
                WriteCsv(root, "EmendasParlamentares_PorFavorecido.csv", 13,
                    Row(13, (0, "EM1"), (2, "Autor"), (5, "2024/01"),
                        (6, AlphaCnpj), (7, "Favorecida"), (12, "80,00")),
                    Row(13, (0, "EM2"), (2, "Autor"), (5, "2024/02"),
                        (6, "12345678900"), (7, "Pessoa"), (12, "20,00")))
            };

            using var payload = await ProcessAndReadAsync(
                "emendas_parlamentares",
                root,
                files,
                AlphaCnpj);
            CollectionAssert.AreEquivalent(
                new[] { "favorecimento" },
                ReadTypes(payload, "emendas"));
            Assert.AreEqual(
                "EM1",
                payload.RootElement.GetProperty("emendas")[0]
                    .GetProperty("codigo_emenda").GetString());
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    [TestMethod]
    public async Task EmendasDocumentos_ShouldKeepOnlyCnpjRows()
    {
        var root = CreateTempRoot();
        try
        {
            var files = new[]
            {
                WriteCsv(root, "2024_EmendasParlamentares_PorDocumento.csv", 48,
                    Row(48, (0, "EM1"), (1, "2024"), (3, "Autor"),
                        (8, "20/01/2024"), (9, "DOC1"), (14, "Pagamento"),
                        (15, AlphaCnpj), (16, "Favorecida"), (46, "Subtítulo"),
                        (47, "Sim")),
                    Row(48, (0, "EM2"), (1, "2024"), (9, "DOC-PF"),
                        (15, "12345678900"), (16, "Pessoa")))
            };

            using var payload = await ProcessAndReadAsync(
                "emendas_documentos",
                root,
                files,
                AlphaCnpj);
            var records = payload.RootElement.GetProperty("documentos");
            Assert.AreEqual(1, records.GetArrayLength());
            Assert.AreEqual(
                "DOC1",
                records[0].GetProperty("codigo_documento").GetString());
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    private static async Task<JsonDocument> ProcessAndReadAsync(
        string datasetKey,
        string root,
        IReadOnlyList<string> paths,
        string cnpj)
    {
        var processor = new PartitionedParquetProcessor(new IntegrationOptions
        {
            DuckDbThreads = 1,
            DuckDbMemoryLimit = "128MB",
            DuckDbMaxTempDirectorySize = "1GB",
            ProcessingPartitions = 4
        });
        var output = Path.Combine(root, "output");
        var result = await processor.ConvertAsync(
            PortalDatasetDefinition.GetRequired(datasetKey),
            new ExtractedDataset(paths),
            output,
            UpdatedAt,
            3);

        Assert.IsTrue(result.RecordCount > 0);
        return await ReadPayloadAsync(result.ParquetGlob, cnpj);
    }

    private static async Task<JsonDocument> ReadPayloadAsync(
        string parquetGlob,
        string cnpj)
    {
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
                              SELECT CAST(payload_json AS VARCHAR)
                              FROM read_parquet('{parquetGlob.Replace("'", "''")}')
                              WHERE cnpj = '{cnpj.Replace("'", "''")}'
                              ORDER BY payload_json
                              """;
        await using var reader = await command.ExecuteReaderAsync();
        var payloads = new List<JsonDocument>();
        while (await reader.ReadAsync())
            payloads.Add(JsonDocument.Parse(reader.GetString(0)));

        if (payloads.Count == 0)
        {
            throw new AssertFailedException(
                $"Payload não encontrado para {cnpj}.");
        }

        if (payloads.Count == 1)
            return payloads[0];

        try
        {
            var collectionProperty = payloads
                .SelectMany(payload =>
                    payload.RootElement.EnumerateObject())
                .First(property =>
                    property.Value.ValueKind == JsonValueKind.Array)
                .Name;
            var updatedAt = payloads
                .Select(payload =>
                    payload.RootElement.GetProperty("updated_at").GetString())
                .Where(value => value is not null)
                .Max(StringComparer.Ordinal);
            var mergedRecords = new JsonArray();
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var record in payloads.SelectMany(payload =>
                         payload.RootElement
                             .GetProperty(collectionProperty)
                             .EnumerateArray()))
            {
                var json = record.GetRawText();
                if (seen.Add(json))
                    mergedRecords.Add(JsonNode.Parse(json));
            }

            return JsonDocument.Parse(new JsonObject
            {
                ["updated_at"] = updatedAt,
                [collectionProperty] = mergedRecords
            }.ToJsonString());
        }
        finally
        {
            foreach (var payload in payloads)
                payload.Dispose();
        }
    }

    private static string?[] ReadTypes(
        JsonDocument payload,
        string property) =>
        payload.RootElement.GetProperty(property)
            .EnumerateArray()
            .Select(record => record.GetProperty("tipo_registro").GetString())
            .ToArray();

    private static string WriteCsv(
        string directory,
        string fileName,
        int columnCount,
        params string[][] rows)
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        Assert.IsTrue(rows.All(row => row.Length == columnCount));
        var header = Enumerable.Range(1, columnCount)
            .Select(index => $"COLUNA {index}")
            .ToArray();
        var content = new[] { header }
            .Concat(rows)
            .Select(row => string.Join(';', row.Select(value =>
                $"\"{value.Replace("\"", "\"\"")}\"")));
        var path = Path.Combine(directory, fileName);
        File.WriteAllText(
            path,
            string.Join("\r\n", content),
            Encoding.GetEncoding(1252));
        return path;
    }

    private static string[] Row(
        int length,
        params (int Index, string Value)[] values)
    {
        var row = Enumerable.Repeat(string.Empty, length).ToArray();
        foreach (var (index, value) in values)
            row[index] = value;
        return row;
    }

    private static string CreateTempRoot()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-portal-partitioned-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempRoot(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
