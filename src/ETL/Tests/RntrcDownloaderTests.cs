using CNPJExporter.Modules.Rntrc.Downloaders;
using CNPJExporter.Modules.Rntrc.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class RntrcDownloaderTests
{
    [TestMethod]
    public void SelectLatestCsvResource_ShouldPickMostRecentCsvResource()
    {
        const string packageJson = """
        {
          "success": true,
          "result": {
            "resources": [
              {
                "name": "Dicionario",
                "format": "PDF",
                "url": "https://example.invalid/dicionario.pdf",
                "last_modified": "2026-04-10T09:09:56.000000",
                "position": 0,
                "size": 123
              },
              {
                "name": "Fev26 - RNTRC",
                "format": "CSV",
                "url": "https://example.invalid/transportadores_rntrc_02_2026.csv",
                "last_modified": "2026-03-10T09:06:23.399918",
                "position": 67,
                "size": 147597801
              },
              {
                "name": "Mar26 - RNTRC",
                "format": "CSV",
                "url": "https://example.invalid/transportadores_rntrc_03_2026.csv",
                "last_modified": "2026-04-10T09:09:56.818094",
                "position": 68,
                "size": 149626928
              }
            ]
          }
        }
        """;

        var source = Downloader.SelectLatestCsvResourceForTest(packageJson);

        Assert.AreEqual("transportadores_rntrc_03_2026.csv", source.FileName);
        Assert.AreEqual("Mar26 - RNTRC", source.DisplayName);
        Assert.AreEqual(149626928, source.ContentLength);
        Assert.AreEqual(DateTimeOffset.Parse("2026-04-10T09:09:56.818094Z"), source.LastModified);
        Assert.IsTrue(
            source.SourceVersion.Length == 64 && source.SourceVersion.All(Uri.IsHexDigit),
            "source_version do RNTRC deve ser sempre um hash estável, não URL/data/tamanho em texto aberto.");
        Assert.IsFalse(source.SourceVersion.Contains("https://", StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task CanReuseLocalFile_ShouldRejectSameSizeCsv_WhenSourceVersionChanged()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-rntrc-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var csvPath = Path.Combine(tempRoot, "transportadores_rntrc_03_2026.csv");
            await File.WriteAllTextAsync(csvPath, "same-size");
            var previous = new SourceFile(
                new Uri("https://example.invalid/transportadores_rntrc_03_2026.csv"),
                "Mar26 - RNTRC",
                "transportadores_rntrc_03_2026.csv",
                "\"etag-old\"",
                new FileInfo(csvPath).Length,
                DateTimeOffset.Parse("2026-04-10T09:09:56Z"));
            var current = previous with
            {
                SourceVersion = "\"etag-new\"",
                LastModified = DateTimeOffset.Parse("2026-04-11T09:09:56Z")
            };

            await Downloader.WriteSourceMetadataForTestAsync(csvPath, previous);

            Assert.IsFalse(
                Downloader.CanReuseLocalFileForTest(csvPath, current),
                "CSV local com mesmo tamanho deve ser baixado novamente quando ETag/LastModified muda.");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }
}
