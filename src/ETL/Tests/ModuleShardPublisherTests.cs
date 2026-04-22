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
                1,
                1,
                "2026-04-13T00:00:00Z",
                "base-release",
                PublishedZipArtifactSnapshot.Missing,
                new Dictionary<string, PublishedModuleShardSnapshot>(StringComparer.Ordinal)
                {
                    ["cno"] = new PublishedModuleShardSnapshot(
                        "cno",
                        "cno",
                        "2",
                        "etag-old",
                        DateTimeOffset.Parse("2026-04-13T00:00:00Z"),
                        10,
                        "module-old",
                        PublishedZipArtifactSnapshot.Missing)
                });

            var exporter = new FakeModuleShardExporter();
            var zipPublisher = new FakeShardZipPublisher();
            var result = await new ModuleShardPublisher(exporter, zipPublisher).PublishAsync(
                "module-new",
                [summary],
                publishedInfo,
                tempRoot);

            var module = result["cno"];
            Assert.AreEqual("etag-new", module.SourceVersion);
            Assert.AreEqual(DateTimeOffset.Parse("2026-04-14T00:00:00Z"), module.UpdatedAt);
            Assert.AreEqual("module-new", module.StorageReleaseId, "Módulo alterado deve publicar todos os shards no novo release único.");
            Assert.AreEqual(1, exporter.CallCount);
            Assert.AreEqual(1, zipPublisher.ModuleCallCount);
            Assert.AreEqual("https://file.opencnpj.org/releases/cno/data.zip", module.Zip.Url);
            Assert.IsNotNull(exporter.LastSource);
            Assert.AreEqual("cno", exporter.LastSource.Key);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task PublishAsync_ShouldSkipUnchangedModule_WhenOnlyZipIsMissing()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-module-zip-skip-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var parquetPath = Path.Combine(tempRoot, "cno.parquet");
            await File.WriteAllTextAsync(parquetPath, "placeholder");
            var descriptor = new DataIntegrationDescriptor("cno", "cno", TimeSpan.FromDays(1), "2");
            var summary = new DataIntegrationRunSummary(
                descriptor,
                "etag-old",
                DateTimeOffset.Parse("2026-04-13T00:00:00Z"),
                parquetPath,
                10,
                [],
                new Dictionary<string, string>(StringComparer.Ordinal));
            var publishedInfo = new PublishedInfoSnapshot(
                1,
                1,
                "2026-04-13T00:00:00Z",
                "base-release",
                PublishedZipArtifactSnapshot.Missing,
                new Dictionary<string, PublishedModuleShardSnapshot>(StringComparer.Ordinal)
                {
                    ["cno"] = new PublishedModuleShardSnapshot(
                        "cno",
                        "cno",
                        "2",
                        "etag-old",
                        DateTimeOffset.Parse("2026-04-13T00:00:00Z"),
                        10,
                        "module-old",
                        PublishedZipArtifactSnapshot.Missing)
                });

            var exporter = new FakeModuleShardExporter();
            var zipPublisher = new FakeShardZipPublisher();
            var result = await new ModuleShardPublisher(exporter, zipPublisher).PublishAsync(
                "module-new",
                [summary],
                publishedInfo,
                tempRoot);

            var module = result["cno"];
            Assert.AreEqual(0, exporter.CallCount, "Módulo sem mudanças não deve republicar shards.");
            Assert.AreEqual(0, zipPublisher.ModuleCallCount, "Módulo sem mudanças não deve gerar ZIP.");
            Assert.AreEqual("module-old", module.StorageReleaseId);
            Assert.IsFalse(module.Zip.Available);
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

    private sealed class FakeShardZipPublisher : IShardZipPublisher
    {
        public int ModuleCallCount { get; private set; }

        public Task<ZipArtifactPublication> PublishBaseAsync(
            string datasetKey,
            string releaseId,
            string outputRootDir,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ZipArtifactPublication> PublishModuleAsync(
            string moduleKey,
            string releaseId,
            string outputRootDir,
            CancellationToken cancellationToken = default)
        {
            ModuleCallCount++;
            return Task.FromResult(new ZipArtifactPublication(
                true,
                1234,
                $"https://file.opencnpj.org/releases/{moduleKey}/data.zip",
                "abc123"));
        }
    }
}
