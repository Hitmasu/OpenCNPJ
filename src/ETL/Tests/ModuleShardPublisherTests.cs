using CNPJExporter.Integrations;
using CNPJExporter.Processors;
using CNPJExporter.Processors.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ModuleShardPublisherTests
{
    [TestMethod]
    public void TryGetCompletedArtifactFiles_ShouldAcceptOnlyCompleteShardPairsAndZip()
    {
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-module-resume-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "607.ndjson"), "{}");
            File.WriteAllText(Path.Combine(tempRoot, "607.index.bin"), "index");
            File.WriteAllText(Path.Combine(tempRoot, "data.zip"), "zip");

            Assert.IsTrue(
                ModuleShardPublisher.TryGetCompletedArtifactFiles(
                    tempRoot,
                    out var files,
                    out var zipPath));
            Assert.AreEqual(2, files.Count);
            Assert.AreEqual(
                Path.Combine(tempRoot, "data.zip"),
                zipPath);

            File.WriteAllText(Path.Combine(tempRoot, "607.ndjson.tmp"), "partial");
            Assert.IsFalse(
                ModuleShardPublisher.TryGetCompletedArtifactFiles(
                    tempRoot,
                    out _,
                    out _));
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

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

    [TestMethod]
    public async Task PublishAsync_ShouldAppendOnlyNewHistoricalSegment()
    {
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-module-segment-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var parquetPath = Path.Combine(tempRoot, "2026-07.parquet");
            await File.WriteAllTextAsync(parquetPath, "placeholder");
            var descriptor = new DataIntegrationDescriptor(
                "licitacoes",
                "licitacoes",
                TimeSpan.FromDays(1),
                "1",
                SegmentCollectionProperty: "licitacoes");
            var segment = new DataIntegrationSegment(
                "2026-07",
                "source-202607",
                DateTimeOffset.Parse("2026-07-23T00:00:00Z"),
                parquetPath,
                12);
            var summary = new DataIntegrationRunSummary(
                descriptor,
                "catalog-v2",
                segment.UpdatedAt,
                ParquetGlob: null,
                RecordCount: 12,
                ChangedCnpjs: [],
                CurrentHashes: new Dictionary<string, string>(
                    StringComparer.Ordinal),
                HasMetadataChanges: true,
                Segments: [segment]);
            var publishedInfo = new PublishedInfoSnapshot(
                1,
                1,
                "2026-06-30T00:00:00Z",
                "base-release",
                PublishedZipArtifactSnapshot.Missing,
                new Dictionary<string, PublishedModuleShardSnapshot>(
                    StringComparer.Ordinal)
                {
                    ["licitacoes"] = new PublishedModuleShardSnapshot(
                        "licitacoes",
                        "licitacoes",
                        "1",
                        "catalog-v1",
                        DateTimeOffset.Parse("2026-06-30T00:00:00Z"),
                        8,
                        "routing-v1",
                        PublishedZipArtifactSnapshot.Missing,
                        RoutingReleaseId: "routing-v1",
                        SegmentCollectionProperty: "licitacoes",
                        Segments:
                        [
                            new PublishedModuleSegmentSnapshot(
                                "2017",
                                "source-2017",
                                DateTimeOffset.Parse(
                                    "2017-12-31T00:00:00Z"),
                                8,
                                "segment-2017",
                                PublishedZipArtifactSnapshot.Missing)
                        ])
                });
            var exporter = new FakeModuleShardExporter();
            var zipPublisher = new FakeShardZipPublisher();
            var routingPublisher = new FakeModuleRoutingPublisher();

            var result = await new ModuleShardPublisher(
                    exporter,
                    zipPublisher,
                    routingPublisher)
                .PublishAsync(
                    "release-202607",
                    [summary],
                    publishedInfo,
                    tempRoot);

            var publication = result["licitacoes"];
            Assert.IsTrue(publication.IsSegmented);
            Assert.AreEqual(
                "release-202607",
                publication.RoutingReleaseId);
            CollectionAssert.AreEqual(
                new[] { "2017", "2026-07" },
                publication.EffectiveSegments
                    .Select(item => item.Id)
                    .ToArray());
            Assert.AreEqual(1, exporter.SegmentCallCount);
            Assert.AreEqual("2026-07", exporter.LastSegment?.Id);
            CollectionAssert.AreEqual(
                new[] { "2026-07" },
                routingPublisher.ChangedSegmentIds.ToArray());
            Assert.AreEqual(
                "routing-v1",
                routingPublisher.PreviousRoutingReleaseId);
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
        public int SegmentCallCount { get; private set; }
        public DataIntegrationShardSource? LastSource { get; private set; }
        public DataIntegrationSegment? LastSegment { get; private set; }

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

        public Task<ModuleShardExportResult> ExportSegmentAndUploadAsync(
            DataIntegrationShardSource source,
            DataIntegrationSegment segment,
            string releaseId,
            string outputRootDir,
            CancellationToken cancellationToken = default)
        {
            SegmentCallCount++;
            LastSource = source;
            LastSegment = segment;
            var localDirectory = Path.Combine(
                outputRootDir,
                "segments",
                segment.Id,
                releaseId);
            Directory.CreateDirectory(localDirectory);
            return Task.FromResult(new ModuleShardExportResult(
                localDirectory,
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

        public Task<ZipArtifactPublication> PublishModuleSegmentAsync(
            string moduleKey,
            string segmentId,
            string releaseId,
            string outputRootDir,
            CancellationToken cancellationToken = default)
        {
            ModuleCallCount++;
            return Task.FromResult(new ZipArtifactPublication(
                true,
                123,
                $"https://file.opencnpj.org/releases/{moduleKey}/segments/{segmentId}/data.zip",
                $"zip-{segmentId}"));
        }
    }

    private sealed class FakeModuleRoutingPublisher
        : IModuleRoutingPublisher
    {
        public string? PreviousRoutingReleaseId { get; private set; }
        public IReadOnlyCollection<string> ChangedSegmentIds { get; private set; }
            = [];

        public Task<ModuleRoutingPublishResult> PublishAsync(
            string moduleKey,
            string releaseId,
            string? previousRoutingReleaseId,
            IReadOnlyDictionary<string, string>
                changedSegmentShardDirectories,
            IReadOnlyCollection<string> removedSegmentIds,
            string outputRootDir,
            CancellationToken cancellationToken = default)
        {
            PreviousRoutingReleaseId = previousRoutingReleaseId;
            ChangedSegmentIds = changedSegmentShardDirectories.Keys.ToArray();
            return Task.FromResult(new ModuleRoutingPublishResult(
                releaseId,
                Path.Combine(outputRootDir, "routing", releaseId),
                ["607"]));
        }
    }
}
