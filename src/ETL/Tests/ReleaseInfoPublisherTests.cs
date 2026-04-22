using System.Text.Json;
using CNPJExporter.Processors;
using CNPJExporter.Processors.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ReleaseInfoPublisherTests
{
    [TestMethod]
    public void SerializeForTest_ShouldEmbedZipMetadata_AtRootAndDatasetLevel()
    {
        var publication = new ReleaseInfoPublication(
            DatasetKey: "2026-04",
            ReceitaDatasetKey: "receita",
            Total: 100,
            ShardCount: 2,
            LastUpdated: "2026-04-22T00:00:00Z",
            ShardPrefixLength: 3,
            StorageReleaseId: "release-123",
            PublicationReleaseId: "release-123",
            BaseZip: new ZipArtifactPublication(
                true,
                2048,
                "https://file.opencnpj.org/releases/receita/data.zip",
                "base-md5"),
            IntegrationSummaries: [],
            ModuleShards: new Dictionary<string, ModuleShardPublication>(StringComparer.Ordinal)
            {
                ["cno"] = new(
                    "cno",
                    "cno",
                    "2",
                    "etag-1",
                    DateTimeOffset.Parse("2026-04-22T00:00:00Z"),
                    10,
                    "module-123",
                    new ZipArtifactPublication(
                        true,
                        512,
                        "https://file.opencnpj.org/releases/cno/data.zip",
                        "module-md5"))
            });

        var json = ReleaseInfoPublisher.SerializeForTest(publication);
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var receita = root.GetProperty("datasets").GetProperty("receita");
        var cno = root.GetProperty("datasets").GetProperty("cno");

        Assert.IsFalse(root.TryGetProperty("zip_available", out _));
        Assert.IsFalse(root.TryGetProperty("zip_size", out _));
        Assert.IsFalse(root.TryGetProperty("zip_url", out _));
        Assert.IsFalse(root.TryGetProperty("zip_md5checksum", out _));
        Assert.AreEqual("per-dataset-shards-v1", root.GetProperty("zip_layout").GetString());

        Assert.IsTrue(receita.GetProperty("zip_available").GetBoolean());
        Assert.AreEqual(2048, receita.GetProperty("zip_size").GetInt64());
        Assert.AreEqual("base-md5", receita.GetProperty("zip_md5checksum").GetString());

        Assert.IsTrue(cno.GetProperty("zip_available").GetBoolean());
        Assert.AreEqual(512, cno.GetProperty("zip_size").GetInt64());
        Assert.AreEqual("https://file.opencnpj.org/releases/cno/data.zip", cno.GetProperty("zip_url").GetString());
        Assert.AreEqual("module-md5", cno.GetProperty("zip_md5checksum").GetString());
    }
}
