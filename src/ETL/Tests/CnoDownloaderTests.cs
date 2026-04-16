using CNPJExporter.Modules.Cno.Downloaders;
using CNPJExporter.Modules.Cno.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class CnoDownloaderTests
{
    [TestMethod]
    public async Task CanReuseLocalFile_ShouldRejectSameSizeZip_WhenSourceVersionChanged()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-cno-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var zipPath = Path.Combine(tempRoot, "cno.zip");
            await File.WriteAllTextAsync(zipPath, "same-size");
            var previous = new SourceFile(
                new Uri("https://example.invalid/cno.zip"),
                "cno.zip",
                "etag-old",
                new FileInfo(zipPath).Length,
                DateTimeOffset.Parse("2026-04-13T00:00:00Z"));
            var current = previous with
            {
                SourceVersion = "etag-new",
                LastModified = DateTimeOffset.Parse("2026-04-14T00:00:00Z")
            };

            await Downloader.WriteSourceMetadataForTestAsync(zipPath, previous);

            Assert.IsFalse(
                Downloader.CanReuseLocalFileForTest(zipPath, current),
                "ZIP local com mesmo tamanho deve ser baixado novamente quando ETag/LastModified muda.");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }
}
