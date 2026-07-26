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

    [TestMethod]
    public async Task GetPublishedInfoAsync_ShouldParseSegmentedModule()
    {
        const string payload = """
            {
              "storage_release_id": "base-release",
              "datasets": {
                "licitacoes": {
                  "json_property_name": "licitacoes",
                  "storage_release_id": "routing-v2",
                  "routing_release_id": "routing-v2",
                  "segment_collection_property": "licitacoes",
                  "schema_version": "1",
                  "source_version": "catalog-v2",
                  "updated_at": "2026-07-23T00:00:00Z",
                  "record_count": 20,
                  "segments": [
                    {
                      "id": "2017",
                      "source_version": "source-2017",
                      "updated_at": "2017-12-31T00:00:00Z",
                      "record_count": 8,
                      "storage_release_id": "segment-2017",
                      "zip_available": true,
                      "zip_size": 100,
                      "zip_url": "https://file.opencnpj.org/releases/licitacoes/segments/2017/data.zip",
                      "zip_md5checksum": "zip-2017"
                    }
                  ]
                }
              }
            }
            """;

        var info = await new PublishedInfoClient(
                new InMemoryPublishedInfoReader(payload))
            .GetPublishedInfoAsync();

        var licitacoes = info.ModuleShards["licitacoes"];
        Assert.AreEqual("routing-v2", licitacoes.RoutingReleaseId);
        Assert.AreEqual(
            "licitacoes",
            licitacoes.SegmentCollectionProperty);
        Assert.AreEqual(1, licitacoes.EffectiveSegments.Count);
        Assert.AreEqual(
            "segment-2017",
            licitacoes.EffectiveSegments[0].StorageReleaseId);
        Assert.IsTrue(licitacoes.EffectiveSegments[0].Zip.Available);
    }

    private sealed class InMemoryPublishedInfoReader(string payload) : PublishedInfoClient.IPublishedInfoReader
    {
        public Task<Stream> OpenReadAsync(CancellationToken cancellationToken) =>
            Task.FromResult<Stream>(new MemoryStream(Encoding.UTF8.GetBytes(payload), writable: false));
    }
}
