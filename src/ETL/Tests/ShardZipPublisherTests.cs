using System.IO.Compression;
using CNPJExporter.Processors;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ShardZipPublisherTests
{
    [TestMethod]
    public async Task BuildLocalZipForTest_ShouldZipOnlyShardFiles_AndReturnMetadata()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-zip-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            await File.WriteAllTextAsync(Path.Combine(tempRoot, "000.ndjson"), "{\"cnpj\":\"1\"}\n");
            await File.WriteAllTextAsync(Path.Combine(tempRoot, "000.index.bin"), "idx");
            await File.WriteAllTextAsync(Path.Combine(tempRoot, "ignore.txt"), "ignore");

            var zipPath = Path.Combine(tempRoot, "data.zip");
            var zip = await ShardZipPublisher.BuildLocalZipForTest(tempRoot, zipPath, "cno");

            Assert.IsTrue(zip.Available);
            Assert.AreEqual("https://file.opencnpj.org/releases/cno/data.zip", zip.Url);
            Assert.IsTrue(zip.Size > 0);
            Assert.IsFalse(string.IsNullOrWhiteSpace(zip.Md5Checksum));

            using var archive = ZipFile.OpenRead(zipPath);
            var entries = archive.Entries.Select(entry => entry.FullName).OrderBy(name => name, StringComparer.Ordinal).ToArray();
            CollectionAssert.AreEqual(new[] { "000.ndjson" }, entries);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }
}
