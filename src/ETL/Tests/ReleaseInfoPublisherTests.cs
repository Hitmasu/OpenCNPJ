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

    [TestMethod]
    public void SerializeForTest_ShouldPublishSegmentRoutingManifest()
    {
        var publication = new ReleaseInfoPublication(
            DatasetKey: "2026-07",
            ReceitaDatasetKey: "receita",
            Total: 100,
            ShardCount: 2,
            LastUpdated: "2026-07-23T00:00:00Z",
            ShardPrefixLength: 3,
            StorageReleaseId: "base-release",
            PublicationReleaseId: "release-202607",
            BaseZip: ZipArtifactPublication.Missing,
            IntegrationSummaries: [],
            ModuleShards: new Dictionary<string, ModuleShardPublication>(
                StringComparer.Ordinal)
            {
                ["licitacoes"] = new(
                    "licitacoes",
                    "licitacoes",
                    "1",
                    "catalog-v2",
                    DateTimeOffset.Parse("2026-07-23T00:00:00Z"),
                    20,
                    "routing-v2",
                    ZipArtifactPublication.Missing,
                    RoutingReleaseId: "routing-v2",
                    SegmentCollectionProperty: "licitacoes",
                    Segments:
                    [
                        new ModuleSegmentPublication(
                            "2017",
                            "source-2017",
                            DateTimeOffset.Parse("2017-12-31T00:00:00Z"),
                            8,
                            "segment-2017",
                            new ZipArtifactPublication(
                                true,
                                100,
                                "https://file.opencnpj.org/releases/licitacoes/segments/2017/data.zip",
                                "zip-2017")),
                        new ModuleSegmentPublication(
                            "2026-07",
                            "source-202607",
                            DateTimeOffset.Parse("2026-07-23T00:00:00Z"),
                            12,
                            "segment-202607",
                            ZipArtifactPublication.Missing)
                    ])
            });

        using var document = JsonDocument.Parse(
            ReleaseInfoPublisher.SerializeForTest(publication));
        var root = document.RootElement;
        var licitacoes = root
            .GetProperty("datasets")
            .GetProperty("licitacoes");

        Assert.AreEqual(
            "per-dataset-segments-v2",
            root.GetProperty("zip_layout").GetString());
        Assert.AreEqual(
            "routing-v2",
            licitacoes.GetProperty("routing_release_id").GetString());
        Assert.AreEqual(
            "licitacoes",
            licitacoes
                .GetProperty("segment_collection_property")
                .GetString());
        Assert.AreEqual(2, licitacoes.GetProperty("segments").GetArrayLength());
        Assert.AreEqual(
            "2017",
            licitacoes.GetProperty("segments")[0].GetProperty("id").GetString());
        Assert.AreEqual(
            "segment-2017",
            licitacoes
                .GetProperty("segments")[0]
                .GetProperty("storage_release_id")
                .GetString());
    }
}
