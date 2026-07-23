using System.Text;
using CNPJExporter.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class PublishedInfoClientTests
{
    [TestMethod]
    public void DefaultConstructor_ShouldReadInfoFromRcloneStorage()
    {
        var client = new PublishedInfoClient();

        Assert.AreEqual(
            typeof(PublishedInfoClient.RclonePublishedInfoReader),
            client.ReaderTypeForTest,
            "O pipeline deve ler info.json diretamente do storage via rclone, não da API pública.");
    }

    [TestMethod]
    public async Task GetPublishedInfoAsync_ShouldParseZipMetadata_FromDatasets()
    {
        const string payload = """
            {
              "total": 10,
              "shard_count": 2,
              "last_updated": "2026-04-22T00:00:00Z",
              "storage_release_id": "base-release",
              "datasets": {
                "receita": {
                  "storage_release_id": "base-release",
                  "zip_available": true,
                  "zip_size": 4096,
                  "zip_url": "https://file.opencnpj.org/releases/receita/data.zip",
                  "zip_md5checksum": "base-md5"
                },
                "cno": {
                  "storage_release_id": "module-release",
                  "json_property_name": "cno",
                  "schema_version": "2",
                  "source_version": "etag-1",
                  "updated_at": "2026-04-22T00:00:00Z",
                  "record_count": 4,
                  "zip_available": true,
                  "zip_size": 512,
                  "zip_url": "https://file.opencnpj.org/releases/cno/data.zip",
                  "zip_md5checksum": "module-md5"
                }
              }
            }
            """;

        var client = new PublishedInfoClient(new InMemoryPublishedInfoReader(payload));
        var info = await client.GetPublishedInfoAsync();

        Assert.AreEqual("base-release", info.StorageReleaseId);
        Assert.IsTrue(info.BaseZip.Available);
        Assert.AreEqual(4096, info.BaseZip.Size);
        Assert.AreEqual("https://file.opencnpj.org/releases/receita/data.zip", info.BaseZip.Url);
        Assert.AreEqual("base-md5", info.BaseZip.Md5Checksum);

        var cno = info.ModuleShards["cno"];
        Assert.AreEqual("module-release", cno.StorageReleaseId);
        Assert.IsTrue(cno.Zip.Available);
        Assert.AreEqual(512, cno.Zip.Size);
        Assert.AreEqual("module-md5", cno.Zip.Md5Checksum);
    }

    private sealed class InMemoryPublishedInfoReader(string payload) : PublishedInfoClient.IPublishedInfoReader
    {
        public Task<Stream> OpenReadAsync(CancellationToken cancellationToken) =>
            Task.FromResult<Stream>(new MemoryStream(Encoding.UTF8.GetBytes(payload), writable: false));
    }
}
