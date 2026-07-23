using CNPJExporter.Integrations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CnoDataIntegration = CNPJExporter.Modules.Cno.DataIntegration;
using CnoSourceFileDownloader = CNPJExporter.Modules.Cno.ICnoSourceFileDownloader;
using CnoIntegrationOptions = CNPJExporter.Modules.Cno.Configuration.IntegrationOptions;
using RntrcDataIntegration = CNPJExporter.Modules.Rntrc.DataIntegration;
using RntrcIntegrationOptions = CNPJExporter.Modules.Rntrc.Configuration.IntegrationOptions;
using RntrcSourceFileDownloader = CNPJExporter.Modules.Rntrc.IRntrcSourceFileDownloader;

namespace ETL.Tests;

[TestClass]
public sealed class DataIntegrationSourceReuseTests
{
    [TestMethod]
    public async Task CnoRunAsync_ShouldSkipDownload_WhenSourceLastModifiedIsNotNewerThanPublishedState()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-cno-reuse-{Guid.NewGuid():N}");
        var parquetPath = Path.Combine(tempRoot, "parquet", "integrations", "cno", "cno.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(parquetPath)!);
        await File.WriteAllTextAsync(parquetPath, "placeholder");

        var downloadCalls = 0;
        var sourceCalls = 0;
        var source = new SourceFile(
            new Uri("https://example.test/cno.zip"),
            "cno.zip",
            "source-from-site",
            ContentLength: 128,
            LastModified: DateTimeOffset.Parse("2026-07-03T04:33:27Z"));
        var integration = new CnoDataIntegration(
            new CnoIntegrationOptions(),
            _ =>
            {
                sourceCalls++;
                return Task.FromResult(source);
            },
            new FakeCnoSourceFileDownloader(
                () => downloadCalls++,
                "CNO não deve baixar quando a fonte não é mais nova que o estado publicado."));
        var previousState = new DataIntegrationHashState(
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["00000000000000"] = "published-hash"
            },
            SourceVersion: "published-source",
            UpdatedAt: DateTimeOffset.Parse("2026-07-04T08:00:00Z"),
            ParquetGlob: parquetPath,
            SchemaVersion: integration.Descriptor.SchemaVersion);

        try
        {
            var result = await RunThroughOrchestratorAsync(tempRoot, integration, previousState);

            Assert.AreEqual(1, sourceCalls);
            Assert.AreEqual(0, downloadCalls);
            Assert.AreEqual("published-source", result.SourceVersion, "Resultado reutilizado deve manter a metadata publicada.");
            Assert.AreEqual(1, result.CurrentHashes.Count);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task RntrcRunAsync_ShouldSkipDownload_WhenSourceVersionAlreadyPublished()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-rntrc-reuse-{Guid.NewGuid():N}");
        var parquetPath = Path.Combine(tempRoot, "parquet", "integrations", "rntrc", "rntrc.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(parquetPath)!);
        await File.WriteAllTextAsync(parquetPath, "placeholder");

        var downloadCalls = 0;
        var sourceCalls = 0;
        var source = new SourceFile(
            new Uri("https://example.test/rntrc.csv"),
            "rntrc.csv",
            "same-source",
            ContentLength: 128,
            LastModified: DateTimeOffset.Parse("2026-06-10T09:06:56Z"),
            DisplayName: "RNTRC");
        var integration = new RntrcDataIntegration(
            new RntrcIntegrationOptions(),
            _ =>
            {
                sourceCalls++;
                return Task.FromResult(source);
            },
            new FakeRntrcSourceFileDownloader(
                () => downloadCalls++,
                "RNTRC não deve baixar quando o sourceVersion já está publicado."));
        var previousState = new DataIntegrationHashState(
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["00000000000000"] = "published-hash"
            },
            SourceVersion: "same-source",
            UpdatedAt: DateTimeOffset.Parse("2026-06-10T09:06:56Z"),
            ParquetGlob: parquetPath,
            SchemaVersion: integration.Descriptor.SchemaVersion);

        try
        {
            var result = await RunThroughOrchestratorAsync(tempRoot, integration, previousState);

            Assert.AreEqual(1, sourceCalls);
            Assert.AreEqual(0, downloadCalls);
            Assert.AreEqual("same-source", result.SourceVersion);
            Assert.AreEqual(1, result.CurrentHashes.Count);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static async Task<DataIntegrationRunSummary> RunThroughOrchestratorAsync(
        string tempRoot,
        IDataIntegration integration,
        DataIntegrationHashState previousState)
    {
        var descriptor = integration.Descriptor;
        var paths = new DataIntegrationPaths(
            Path.Combine(tempRoot, "data"),
            Path.Combine(tempRoot, "parquet"),
            Path.Combine(tempRoot, "output"),
            Path.Combine(tempRoot, "downloads"));
        var publishedStates = new Dictionary<string, DataIntegrationPublishedState>(StringComparer.Ordinal)
        {
            [descriptor.Key] = new(
                previousState.SourceVersion,
                DateTimeOffset.Parse("2000-01-01T00:00:00Z"),
                descriptor.SchemaVersion)
        };
        var orchestrator = new DataIntegrationOrchestrator(
            [integration],
            new FakeStateStore(previousState),
            publishedStates);

        var summaries = await orchestrator.RunAsync("2026-06", paths);
        return summaries.Single();
    }

    private sealed class FakeCnoSourceFileDownloader(
        Action onDownload,
        string downloadFailureMessage) : CnoSourceFileDownloader
    {
        public Task<string> DownloadIfNeededAsync(
            SourceFile sourceFile,
            string destinationDirectory,
            CancellationToken cancellationToken = default)
        {
            onDownload();
            throw new AssertFailedException(downloadFailureMessage);
        }
    }

    private sealed class FakeRntrcSourceFileDownloader(
        Action onDownload,
        string downloadFailureMessage) : RntrcSourceFileDownloader
    {
        public Task<string> DownloadIfNeededAsync(
            SourceFile sourceFile,
            string destinationDirectory,
            CancellationToken cancellationToken = default)
        {
            onDownload();
            throw new AssertFailedException(downloadFailureMessage);
        }
    }

    private sealed class FakeStateStore(DataIntegrationHashState state) : IDataIntegrationStateStore
    {
        public Task<DataIntegrationHashState> LoadAsync(
            DataIntegrationDescriptor descriptor,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(state);

        public Task SaveAsync(
            DataIntegrationDescriptor descriptor,
            DataIntegrationHashState stateToSave,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
