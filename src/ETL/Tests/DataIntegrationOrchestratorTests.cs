using CNPJExporter.Integrations;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class DataIntegrationOrchestratorTests
{
    [TestMethod]
    public void ComputeChangedCnpjs_ShouldDetect_AddedUpdatedAndDeletedHashes()
    {
        var previous = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["00000000000001"] = "same",
            ["00000000000002"] = "old",
            ["00000000000003"] = "deleted"
        };
        var current = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["00000000000001"] = "same",
            ["00000000000002"] = "new",
            ["00000000000004"] = "added"
        };

        var changed = DataIntegrationOrchestrator.ComputeChangedCnpjs(previous, current);

        CollectionAssert.AreEqual(
            new[] { "00000000000002", "00000000000003", "00000000000004" },
            changed.ToArray());
    }

    [TestMethod]
    public void DescriptorValidate_ShouldReject_NonIdentifierJsonProperty()
    {
        var descriptor = new DataIntegrationDescriptor(
            "cno",
            "cno-field",
            TimeSpan.FromDays(1),
            "1");

        Assert.ThrowsException<ArgumentException>(descriptor.Validate);
    }

    [TestMethod]
    public async Task RunAsync_ShouldRerunIntegration_WhenCachedParquetGlobIsMissing()
    {
        var descriptor = new DataIntegrationDescriptor("cno", "cno", TimeSpan.FromDays(1), "1");
        var stateStore = new FakeStateStore(new DataIntegrationHashState(
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["00000000000000"] = "old"
            },
            "previous",
            DateTimeOffset.UtcNow,
            Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"), "*.parquet")));
        var integration = new FakeIntegration(
            descriptor,
            new DataIntegrationRunResult(
                "current",
                DateTimeOffset.UtcNow,
                null,
                0,
                new Dictionary<string, string>(StringComparer.Ordinal)));
        var orchestrator = new DataIntegrationOrchestrator([integration], stateStore);
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-integration-test-{Guid.NewGuid():N}");

        try
        {
            var paths = new DataIntegrationPaths(
                Path.Combine(tempRoot, "data"),
                Path.Combine(tempRoot, "parquet"),
                Path.Combine(tempRoot, "output"),
                Path.Combine(tempRoot, "downloads"));

            await orchestrator.RunAsync("2026-03", paths);

            Assert.AreEqual(1, integration.RunCount, "Integração deve rematerializar quando o Parquet cacheado não existe localmente.");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task RunAsync_ShouldRerunIntegration_WhenSchemaVersionChanged()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-integration-test-{Guid.NewGuid():N}");
        var parquetPath = Path.Combine(tempRoot, "parquet", "integrations", "cno", "cno.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(parquetPath)!);
        await File.WriteAllTextAsync(parquetPath, "placeholder");

        try
        {
            var descriptor = new DataIntegrationDescriptor("cno", "cno", TimeSpan.FromDays(1), "2");
            var stateStore = new FakeStateStore(new DataIntegrationHashState(
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["00000000000000"] = "old"
                },
                "same-source",
                DateTimeOffset.UtcNow,
                parquetPath,
                "1"));
            var integration = new FakeIntegration(
                descriptor,
                new DataIntegrationRunResult(
                    "same-source",
                    DateTimeOffset.UtcNow,
                    parquetPath,
                    1,
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["00000000000000"] = "new"
                    }));
            var orchestrator = new DataIntegrationOrchestrator([integration], stateStore);
            var paths = new DataIntegrationPaths(
                Path.Combine(tempRoot, "data"),
                Path.Combine(tempRoot, "parquet"),
                Path.Combine(tempRoot, "output"),
                Path.Combine(tempRoot, "downloads"));

            await orchestrator.RunAsync("2026-03", paths);

            Assert.AreEqual(1, integration.RunCount, "Integração deve rematerializar quando o schema muda.");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task RunAsync_ShouldDeferStatePersistence_UntilPersistStateAsync()
    {
        var descriptor = new DataIntegrationDescriptor("cno", "cno", TimeSpan.FromDays(1), "1");
        var stateStore = new FakeStateStore(DataIntegrationHashState.Empty);
        var integration = new FakeIntegration(
            descriptor,
            new DataIntegrationRunResult(
                "current",
                DateTimeOffset.Parse("2026-04-14T00:00:00Z"),
                "/tmp/cno.parquet",
                1,
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["00000000000000"] = "hash"
                }));
        var orchestrator = new DataIntegrationOrchestrator([integration], stateStore);
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-integration-test-{Guid.NewGuid():N}");

        try
        {
            var paths = new DataIntegrationPaths(
                Path.Combine(tempRoot, "data"),
                Path.Combine(tempRoot, "parquet"),
                Path.Combine(tempRoot, "output"),
                Path.Combine(tempRoot, "downloads"));

            var summaries = await orchestrator.RunAsync("2026-03", paths);

            Assert.AreEqual(0, stateStore.SaveCount, "RunAsync não deve publicar o estado antes dos shards e info.json.");

            await orchestrator.PersistStateAsync(summaries);

            Assert.AreEqual(1, stateStore.SaveCount, "Estado deve ser salvo somente após commit explícito do pipeline.");
            Assert.AreEqual("current", stateStore.LastSavedState?.SourceVersion);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task RunAsync_ShouldUsePublishedInfoAsSourceOfTruth_WhenHashtableIsAhead()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-integration-test-{Guid.NewGuid():N}");
        var parquetPath = Path.Combine(tempRoot, "parquet", "integrations", "cno", "cno.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(parquetPath)!);
        await File.WriteAllTextAsync(parquetPath, "placeholder");

        try
        {
            var descriptor = new DataIntegrationDescriptor("cno", "cno", TimeSpan.FromDays(1), "2");
            var stateStore = new FakeStateStore(new DataIntegrationHashState(
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["00000000000000"] = "already-current"
                },
                "new-source",
                DateTimeOffset.UtcNow,
                parquetPath,
                "2"));
            var integration = new FakeIntegration(
                descriptor,
                new DataIntegrationRunResult(
                    "new-source",
                    DateTimeOffset.UtcNow,
                    parquetPath,
                    1,
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["00000000000000"] = "already-current"
                    }));
            var orchestrator = new DataIntegrationOrchestrator(
                [integration],
                stateStore,
                new Dictionary<string, DataIntegrationPublishedState>(StringComparer.Ordinal)
                {
                    ["cno"] = new("old-source", DateTimeOffset.UtcNow.AddDays(-2), "2")
                });
            var paths = new DataIntegrationPaths(
                Path.Combine(tempRoot, "data"),
                Path.Combine(tempRoot, "parquet"),
                Path.Combine(tempRoot, "output"),
                Path.Combine(tempRoot, "downloads"));

            var summaries = await orchestrator.RunAsync("2026-03", paths);

            Assert.AreEqual(1, integration.RunCount, "Integração deve rodar quando a hashtable não corresponde ao info.json publicado.");
            Assert.IsTrue(summaries[0].RequiresFullPublish, "Hashtable adiantada não pode ser usada para publicação incremental.");
            CollectionAssert.AreEqual(
                new[] { "00000000000000" },
                summaries[0].ChangedCnpjs.ToArray(),
                "Comparação deve partir do info.json publicado; sem hashtable compatível, o CNPJ atual deve ser republicado.");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task RunAsync_ShouldUsePublishedInfoDate_ForRefreshDecision()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-integration-test-{Guid.NewGuid():N}");
        var parquetPath = Path.Combine(tempRoot, "parquet", "integrations", "cno", "cno.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(parquetPath)!);
        await File.WriteAllTextAsync(parquetPath, "placeholder");

        try
        {
            var descriptor = new DataIntegrationDescriptor("cno", "cno", TimeSpan.FromHours(1), "2");
            var stateStore = new FakeStateStore(new DataIntegrationHashState(
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["00000000000000"] = "same"
                },
                "same-source",
                DateTimeOffset.UtcNow,
                parquetPath,
                "2"));
            var integration = new FakeIntegration(
                descriptor,
                new DataIntegrationRunResult(
                    "same-source",
                    DateTimeOffset.UtcNow,
                    parquetPath,
                    1,
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["00000000000000"] = "same"
                    }));
            var orchestrator = new DataIntegrationOrchestrator(
                [integration],
                stateStore,
                new Dictionary<string, DataIntegrationPublishedState>(StringComparer.Ordinal)
                {
                    ["cno"] = new("same-source", DateTimeOffset.UtcNow.AddHours(-2), "2")
                });
            var paths = new DataIntegrationPaths(
                Path.Combine(tempRoot, "data"),
                Path.Combine(tempRoot, "parquet"),
                Path.Combine(tempRoot, "output"),
                Path.Combine(tempRoot, "downloads"));

            var summaries = await orchestrator.RunAsync("2026-03", paths);

            Assert.AreEqual(1, integration.RunCount, "Decisão de refresh deve usar a data do info.json, não a data da hashtable.");
            Assert.IsFalse(summaries[0].RequiresFullPublish, "Hashtable compatível com o info.json pode continuar permitindo publicação incremental.");
            Assert.AreEqual(0, summaries[0].ChangedCnpjs.Count);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task RunAsync_ShouldReuseAndPreserveSegmentStateWithoutGlobalCnpjHashes()
    {
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-segment-state-{Guid.NewGuid():N}");
        var segmentDirectory = Path.Combine(
            tempRoot,
            "parquet",
            "integrations",
            "licitacoes",
            "segments",
            "2024");
        Directory.CreateDirectory(segmentDirectory);
        await File.WriteAllTextAsync(
            Path.Combine(segmentDirectory, "part-000.parquet"),
            "placeholder");
        var segmentGlob = Path.Combine(segmentDirectory, "*.parquet");
        var descriptor = new DataIntegrationDescriptor(
            "licitacoes",
            "licitacoes",
            TimeSpan.FromHours(1),
            "1",
            "licitacoes");
        var segment = new DataIntegrationSegment(
            "2024",
            "segment-v1",
            DateTimeOffset.Parse("2025-01-01T00:00:00Z"),
            segmentGlob,
            10);
        var state = new DataIntegrationHashState(
            new Dictionary<string, string>(StringComparer.Ordinal),
            "catalog-v1",
            DateTimeOffset.Parse("2026-07-20T00:00:00Z"),
            null,
            "1",
            [segment]);
        var source = new SourceFile(
            new Uri("https://example.test/licitacoes/202607"),
            "202607_Licitacoes.zip",
            "catalog-v1",
            null,
            DateTimeOffset.Parse("2026-07-20T00:00:00Z"));
        var integration = new FakeSourceIntegration(
            descriptor,
            source);
        var orchestrator = new DataIntegrationOrchestrator(
            [integration],
            new FakeStateStore(state),
            new Dictionary<string, DataIntegrationPublishedState>(
                StringComparer.Ordinal)
            {
                ["licitacoes"] = new(
                    "catalog-v1",
                    DateTimeOffset.UtcNow.AddHours(-2),
                    "1")
            });

        try
        {
            var summaries = await orchestrator.RunAsync(
                "2026-07",
                new DataIntegrationPaths(
                    Path.Combine(tempRoot, "data"),
                    Path.Combine(tempRoot, "parquet"),
                    Path.Combine(tempRoot, "output"),
                    Path.Combine(tempRoot, "downloads")));

            Assert.AreEqual(
                0,
                integration.RunCount,
                "O mesmo catálogo deve reutilizar os segmentos existentes.");
            Assert.AreEqual(1, summaries.Single().Segments?.Count);
            Assert.AreEqual(
                segmentGlob,
                summaries.Single().Segments?.Single().ParquetGlob);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task RunAsync_ShouldReuseHashlessSnapshotWhenSourceIsUnchanged()
    {
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-snapshot-state-{Guid.NewGuid():N}");
        var parquetDirectory = Path.Combine(
            tempRoot,
            "parquet",
            "integrations",
            "convenios",
            "convenios-parts");
        Directory.CreateDirectory(parquetDirectory);
        await File.WriteAllTextAsync(
            Path.Combine(parquetDirectory, "part-000.parquet"),
            "placeholder");
        var parquetGlob = Path.Combine(parquetDirectory, "*.parquet");
        var descriptor = new DataIntegrationDescriptor(
            "convenios",
            "convenios",
            TimeSpan.FromHours(1),
            "1");
        var state = new DataIntegrationHashState(
            new Dictionary<string, string>(StringComparer.Ordinal),
            "snapshot-v1",
            DateTimeOffset.Parse("2026-07-20T00:00:00Z"),
            parquetGlob,
            "1");
        var source = new SourceFile(
            new Uri("https://example.test/convenios/202607"),
            "202607_Convenios.zip",
            "snapshot-v1",
            null,
            DateTimeOffset.Parse("2026-07-20T00:00:00Z"));
        var integration = new FakeSourceIntegration(
            descriptor,
            source);
        var orchestrator = new DataIntegrationOrchestrator(
            [integration],
            new FakeStateStore(state),
            new Dictionary<string, DataIntegrationPublishedState>(
                StringComparer.Ordinal)
            {
                ["convenios"] = new(
                    "snapshot-v1",
                    DateTimeOffset.UtcNow.AddHours(-2),
                    "1")
            });

        try
        {
            var summaries = await orchestrator.RunAsync(
                "2026-07",
                new DataIntegrationPaths(
                    Path.Combine(tempRoot, "data"),
                    Path.Combine(tempRoot, "parquet"),
                    Path.Combine(tempRoot, "output"),
                    Path.Combine(tempRoot, "downloads")));

            Assert.AreEqual(
                0,
                integration.RunCount,
                "O mesmo snapshot deve reutilizar os Parquets existentes.");
            Assert.AreEqual(
                parquetGlob,
                summaries.Single().ParquetGlob);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private sealed class FakeIntegration(
        DataIntegrationDescriptor descriptor,
        DataIntegrationRunResult result) : IDataIntegration
    {
        public int RunCount { get; private set; }

        public DataIntegrationDescriptor Descriptor => descriptor;

        public Task<DataIntegrationRunResult> RunAsync(
            DataIntegrationRunContext context,
            CancellationToken cancellationToken = default)
        {
            RunCount++;
            return Task.FromResult(result);
        }
    }

    private sealed class FakeSourceIntegration(
        DataIntegrationDescriptor descriptor,
        SourceFile source) : IDataIntegration, IDataIntegrationSourceProvider
    {
        public int RunCount { get; private set; }

        public DataIntegrationDescriptor Descriptor => descriptor;

        public Task<SourceFile> GetSourceAsync(
            CancellationToken cancellationToken = default) =>
            Task.FromResult(source);

        public Task<DataIntegrationRunResult> RunAsync(
            DataIntegrationRunContext context,
            CancellationToken cancellationToken = default)
        {
            RunCount++;
            throw new AssertFailedException(
                "A integração segmentada não deveria ser rematerializada.");
        }
    }

    private sealed class FakeStateStore(DataIntegrationHashState state) : IDataIntegrationStateStore
    {
        public int SaveCount { get; private set; }
        public DataIntegrationHashState? LastSavedState { get; private set; }

        public Task<DataIntegrationHashState> LoadAsync(
            DataIntegrationDescriptor descriptor,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(state);

        public Task SaveAsync(
            DataIntegrationDescriptor descriptor,
            DataIntegrationHashState stateToSave,
            CancellationToken cancellationToken = default)
        {
            SaveCount++;
            LastSavedState = stateToSave;
            return Task.CompletedTask;
        }
    }
}
