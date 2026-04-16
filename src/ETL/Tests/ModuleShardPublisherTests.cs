using CNPJExporter.Integrations;
using CNPJExporter.Processors;
using CNPJExporter.Processors.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ModuleShardPublisherTests
{
    [TestMethod]
    public async Task PublishAsync_ShouldUpdateMetadata_WhenSourceVersionChangedWithoutHashChanges()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-module-publisher-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var parquetPath = Path.Combine(tempRoot, "cno.parquet");
            await File.WriteAllTextAsync(parquetPath, "placeholder");
            var descriptor = new DataIntegrationDescriptor("cno", "cno", TimeSpan.FromDays(1), "2");
            var summary = new DataIntegrationRunSummary(
                descriptor,
                "etag-new",
                DateTimeOffset.Parse("2026-04-14T00:00:00Z"),
                parquetPath,
                10,
                [],
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["60701190000104"] = "same"
                },
                HasMetadataChanges: true);
            var publishedInfo = new PublishedInfoSnapshot(
                Total: 1,
                ShardCount: 1,
                LastUpdated: "2026-04-13T00:00:00Z",
                StorageReleaseId: "base-release",
                DefaultShardReleaseId: "base-release",
                ShardReleases: new Dictionary<string, string>(StringComparer.Ordinal),
                ModuleShards: new Dictionary<string, PublishedModuleShardSnapshot>(StringComparer.Ordinal)
                {
                    ["cno"] = new PublishedModuleShardSnapshot(
                        "cno",
                        "cno",
                        "2",
                        "etag-old",
                        DateTimeOffset.Parse("2026-04-13T00:00:00Z"),
                        10,
                        "module-old",
                        "module-default",
                        new Dictionary<string, string>(StringComparer.Ordinal))
                });

            var result = await new ModuleShardPublisher().PublishAsync(
                "module-new",
                [summary],
                publishedInfo,
                tempRoot);

            var module = result["cno"];
            Assert.AreEqual("etag-new", module.SourceVersion);
            Assert.AreEqual(DateTimeOffset.Parse("2026-04-14T00:00:00Z"), module.UpdatedAt);
            Assert.AreEqual("module-new", module.StorageReleaseId, "O release de publicação deve aparecer em /info para validação.");
            Assert.AreEqual("module-default", module.DefaultShardReleaseId, "Shards existentes devem continuar apontando para o release default anterior.");
            Assert.AreEqual(0, module.ShardReleases.Count, "Refresh sem hash change não deve remapear prefixos.");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }
}
