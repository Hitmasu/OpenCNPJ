using System.IO.Compression;
using System.Text;
using CNPJExporter.Integrations;
using CNPJExporter.Modules.PortalTransparencia;
using CNPJExporter.Modules.PortalTransparencia.Configuration;
using CNPJExporter.Modules.PortalTransparencia.Downloaders;
using CNPJExporter.Modules.PortalTransparencia.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class PortalTransparenciaDownloaderTests
{
    [TestMethod]
    public void SelectLatestArtifact_ShouldUseLatestOfficialCatalogEntry()
    {
        const string html = """
            <script>
                var arquivos = [];
                arquivos.push({"ano" : "2026", "mes" : "05", "dia" : "", "origem" :  "FavorecidosPJ"});
                arquivos.push({"ano" : "2026", "mes" : "06", "dia" : "", "origem" :  "FavorecidosPJ"});
                arquivos.push({"ano" : "2026", "mes" : "07", "dia" : "23", "origem" :  "CEIS"});
            </script>
            """;

        var artifact = Downloader.SelectLatestArtifactForTest(
            html,
            new Uri("https://portaldatransparencia.gov.br/download-de-dados"),
            "favorecidos_pj");

        Assert.AreEqual("202606", artifact.DateToken);
        Assert.AreEqual("FavorecidosPJ", artifact.ArchiveToken);
        Assert.AreEqual("202606_FavorecidosPJ.zip", artifact.FileName);
        Assert.AreEqual(
            "https://portaldatransparencia.gov.br/download-de-dados/favorecidos-pj/202606",
            artifact.DownloadUri.AbsoluteUri);
    }

    [TestMethod]
    public void CreateEnabled_ShouldCreateOneIndependentIntegrationPerDataset()
    {
        var integrations = DataIntegration.CreateEnabled(new IntegrationOptions
        {
            EnabledDatasets =
            [
                "favorecidos-pj",
                "ceis",
                "cepim",
                "cnep",
                "acordos-leniencia",
                "licitacoes",
                "contratos",
                "renuncias_fiscais",
                "notas_fiscais",
                "convenios",
                "emendas_parlamentares",
                "emendas_documentos"
            ]
        });

        CollectionAssert.AreEqual(
            new[]
            {
                "favorecidos_pj",
                "ceis",
                "cepim",
                "cnep",
                "acordos_leniencia",
                "licitacoes",
                "contratos",
                "renuncias_fiscais",
                "notas_fiscais",
                "convenios",
                "emendas_parlamentares",
                "emendas_documentos"
            },
            integrations.Select(integration => integration.Descriptor.Key).ToArray());
        Assert.IsTrue(
            integrations.All(integration =>
                integration is IDataIntegrationSourceProvider
                && integration.Descriptor.JsonPropertyName == integration.Descriptor.Key));
    }

    [TestMethod]
    public void HistoricalArtifacts_ShouldUseAnnualClosedYearsAndMonthlyCurrentYear()
    {
        const string html = """
            <script>
                var arquivos = [];
                arquivos.push({"ano" : "2012", "mes" : "12", "dia" : "", "origem" : "Licitacoes"});
                arquivos.push({"ano" : "2013", "mes" : "01", "dia" : "", "origem" : "Licitacoes"});
                arquivos.push({"ano" : "2013", "mes" : "02", "dia" : "", "origem" : "Licitacoes"});
                arquivos.push({"ano" : "2018", "mes" : "12", "dia" : "", "origem" : "Licitacoes"});
                arquivos.push({"ano" : "2026", "mes" : "01", "dia" : "", "origem" : "Licitacoes"});
                arquivos.push({"ano" : "2026", "mes" : "02", "dia" : "", "origem" : "Licitacoes"});
            </script>
            """;
        var baseUri = new Uri("https://portaldatransparencia.gov.br/download-de-dados");
        var artifacts = Downloader.SelectArtifactsForTest(
            html,
            baseUri,
            "licitacoes");

        var segments = Downloader.GroupHistoricalArtifactsForTest(
            artifacts,
            "licitacoes",
            2026);

        CollectionAssert.AreEqual(
            new[] { "2013", "2026-01", "2026-02" },
            segments.Select(segment => segment.Id).ToArray());
        CollectionAssert.AreEqual(
            new[] { "201301", "201302" },
            segments[0].Artifacts.Select(artifact => artifact.DateToken).ToArray());
        Assert.IsFalse(
            segments.SelectMany(segment => segment.Artifacts)
                .Any(artifact => artifact.DateToken == "201812"),
            "201812 deve ficar explicitamente fora porque o ZIP oficial está truncado.");
        CollectionAssert.Contains(
            segments[0].ReplacesSegmentIds.ToArray(),
            "2013-12");
    }

    [TestMethod]
    public void AnnualHistory_ShouldKeepOneSegmentPerYearStartingIn2013()
    {
        const string html = """
            <script>
                var arquivos = [];
                arquivos.push({"ano" : "2012", "mes" : "", "dia" : "", "origem" : "EmendasParlamentaresPorDocumento"});
                arquivos.push({"ano" : "2014", "mes" : "", "dia" : "", "origem" : "EmendasParlamentaresPorDocumento"});
                arquivos.push({"ano" : "2026", "mes" : "", "dia" : "", "origem" : "EmendasParlamentaresPorDocumento"});
            </script>
            """;
        var artifacts = Downloader.SelectArtifactsForTest(
            html,
            new Uri("https://portaldatransparencia.gov.br/download-de-dados"),
            "emendas_documentos");

        var segments = Downloader.GroupHistoricalArtifactsForTest(
            artifacts,
            "emendas_documentos",
            2026);

        CollectionAssert.AreEqual(
            new[] { "2014", "2026" },
            segments.Select(segment => segment.Id).ToArray());
        Assert.IsTrue(segments.All(segment => segment.ReplacesSegmentIds.Count == 0));
    }

    [TestMethod]
    public void SingletonDataset_ShouldResolveOfficialUnicoDownloadRoute()
    {
        var artifact = Downloader.SelectLatestArtifactForTest(
            "<html></html>",
            new Uri("https://portaldatransparencia.gov.br/download-de-dados"),
            "emendas_parlamentares");

        Assert.AreEqual("UNICO", artifact.DateToken);
        Assert.AreEqual(
            "https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/UNICO",
            artifact.DownloadUri.AbsoluteUri);
        Assert.AreEqual("EmendasParlamentares.zip", artifact.FileName);
    }

    [TestMethod]
    public async Task CanReuseLocalFile_ShouldRequireMatchingSourceMetadata()
    {
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-portal-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var zipPath = Path.Combine(tempRoot, "20260723_CEIS.zip");
            await File.WriteAllTextAsync(zipPath, "zip-content");
            var source = new SourceFile(
                new Uri("https://example.invalid/ceis/20260723"),
                Path.GetFileName(zipPath),
                "source-v1",
                new FileInfo(zipPath).Length,
                DateTimeOffset.Parse("2026-07-23T16:22:32Z"));

            await Downloader.WriteSourceMetadataForTestAsync(zipPath, source);

            Assert.IsTrue(Downloader.CanReuseLocalFileForTest(zipPath, source));
            Assert.IsFalse(
                Downloader.CanReuseLocalFileForTest(
                    zipPath,
                    source with { SourceVersion = "source-v2" }));
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAndExtract_ShouldReadCp437ZipEntryNames()
    {
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-portal-unicode-zip-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var source = new SourceFile(
                new Uri("https://example.invalid/renuncias/2015"),
                "2015_RenunciasFiscais.zip",
                "source-v1",
                null,
                null);
            var zipPath = Path.Combine(tempRoot, source.FileName);
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            using (var archive = ZipFile.Open(
                       zipPath,
                       ZipArchiveMode.Create,
                       Encoding.GetEncoding(437)))
            {
                foreach (var fileName in new[]
                         {
                             "2015_RenúnciasFiscais.csv",
                             "2015_EmpresasHabilitadas.csv",
                             "2015_EmpresasImunesOuIsentas.csv",
                             "2015_RenúnciasFiscaisPorBeneficiário.csv"
                         })
                {
                    var entry = archive.CreateEntry(fileName);
                    await using var output = entry.Open();
                    await using var writer = new StreamWriter(output);
                    await writer.WriteAsync("cabecalho");
                }
            }

            await Downloader.WriteSourceMetadataForTestAsync(zipPath, source);
            var downloader = new Downloader(
                new IntegrationOptions(),
                PortalDatasetDefinition.GetRequired("renuncias_fiscais"));

            var extracted = await downloader.DownloadAndExtractAsync(
                source,
                tempRoot);

            CollectionAssert.AreEquivalent(
                new[]
                {
                    "2015_RenúnciasFiscais.csv",
                    "2015_EmpresasHabilitadas.csv",
                    "2015_EmpresasImunesOuIsentas.csv",
                    "2015_RenúnciasFiscaisPorBeneficiário.csv"
                },
                extracted.CsvPaths.Select(Path.GetFileName).ToArray());
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }
}
