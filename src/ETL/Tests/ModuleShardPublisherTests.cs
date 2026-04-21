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
                ModuleShards: new Dictionary<string, PublishedModuleShardSnapshot>(StringComparer.Ordinal)
                {
                    ["cno"] = new PublishedModuleShardSnapshot(
                        "cno",
                        "cno",
                        "2",
                        "etag-old",
                        DateTimeOffset.Parse("2026-04-13T00:00:00Z"),
                        10,
                        "module-old")
                });

            var exporter = new FakeModuleShardExporter();
            var result = await new ModuleShardPublisher(exporter).PublishAsync(
                "module-new",
                [summary],
                publishedInfo,
                tempRoot);

            var module = result["cno"];
            Assert.AreEqual("etag-new", module.SourceVersion);
            Assert.AreEqual(DateTimeOffset.Parse("2026-04-14T00:00:00Z"), module.UpdatedAt);
            Assert.AreEqual("module-new", module.StorageReleaseId, "Módulo alterado deve publicar todos os shards no novo release único.");
            Assert.AreEqual(1, exporter.CallCount);
            Assert.IsNotNull(exporter.LastSource);
            Assert.AreEqual("cno", exporter.LastSource.Key);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private sealed class FakeModuleShardExporter : IModuleShardExporter
    {
        public int CallCount { get; private set; }
        public DataIntegrationShardSource? LastSource { get; private set; }

        public Task<ModuleShardExportResult> ExportAndUploadAsync(
            DataIntegrationShardSource source,
            string releaseId,
            string outputRootDir,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            LastSource = source;
            return Task.FromResult(new ModuleShardExportResult(
                Path.Combine(outputRootDir, "shards", "modules", source.Key, "releases", releaseId),
                ["607"]));
        }
    }
}
