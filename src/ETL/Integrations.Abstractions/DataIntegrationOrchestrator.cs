namespace CNPJExporter.Integrations;

public sealed class DataIntegrationOrchestrator
{
    private readonly IReadOnlyList<IDataIntegration> _integrations;
    private readonly IDataIntegrationStateStore _stateStore;
    private readonly IReadOnlyDictionary<string, DataIntegrationPublishedState> _publishedStates;

    public DataIntegrationOrchestrator(
        IEnumerable<IDataIntegration> integrations,
        IDataIntegrationStateStore stateStore,
        IReadOnlyDictionary<string, DataIntegrationPublishedState>? publishedStates = null)
    {
        _integrations = integrations.ToArray();
        _stateStore = stateStore;
        _publishedStates = publishedStates ?? new Dictionary<string, DataIntegrationPublishedState>(StringComparer.Ordinal);
    }

    public async Task<IReadOnlyList<DataIntegrationRunSummary>> RunAsync(
        string datasetKey,
        DataIntegrationPaths paths,
        CancellationToken cancellationToken = default)
    {
        var summaries = new List<DataIntegrationRunSummary>(_integrations.Count);
        var now = DateTimeOffset.UtcNow;

        foreach (var integration in _integrations)
        {
            var descriptor = integration.Descriptor;
            descriptor.Validate();

            var cachedState = await _stateStore.LoadAsync(descriptor, cancellationToken);
            _publishedStates.TryGetValue(descriptor.Key, out var publishedState);
            var cacheMatchesPublishedInfo = CacheMatchesPublishedInfo(cachedState, publishedState);
            var previousState = cacheMatchesPublishedInfo ? cachedState : DataIntegrationHashState.Empty;
            var moduleWorkDir = Path.Combine(paths.DataDir, "integrations", descriptor.Key);
            var moduleParquetDir = Path.Combine(paths.ParquetDir, "integrations", descriptor.Key);
            Directory.CreateDirectory(moduleWorkDir);
            Directory.CreateDirectory(moduleParquetDir);

            var schemaChanged = !string.Equals(previousState.SchemaVersion, descriptor.SchemaVersion, StringComparison.Ordinal);
            var shouldRun = publishedState is null
                            || !cacheMatchesPublishedInfo
                            || !string.Equals(publishedState.SchemaVersion, descriptor.SchemaVersion, StringComparison.Ordinal)
                            || now - publishedState.UpdatedAt >= descriptor.RefreshInterval
                            || !ParquetGlobExists(cachedState.ParquetGlob);
            var result = shouldRun
                ? await integration.RunAsync(
                    new DataIntegrationRunContext(
                        datasetKey,
                        paths,
                        moduleWorkDir,
                        moduleParquetDir,
                        previousState,
                        now),
                    cancellationToken)
                : DataIntegrationRunResult.Unchanged(previousState, now);

            var changedCnpjs = ComputeChangedCnpjs(previousState.Hashes, result.CurrentHashes);
            var sourceVersionChanged = !string.Equals(previousState.SourceVersion, result.SourceVersion, StringComparison.Ordinal);
            var metadataChanged = schemaChanged || sourceVersionChanged;
            var currentState = new DataIntegrationHashState(
                new Dictionary<string, string>(result.CurrentHashes, StringComparer.Ordinal),
                result.SourceVersion,
                result.UpdatedAt,
                result.ParquetGlob,
                descriptor.SchemaVersion);
            var shouldPersistState = shouldRun || changedCnpjs.Count > 0 || metadataChanged;

            summaries.Add(new DataIntegrationRunSummary(
                descriptor,
                result.SourceVersion,
                result.UpdatedAt,
                result.ParquetGlob,
                result.RecordCount,
                changedCnpjs,
                currentState.Hashes,
                RequiresFullPublish: publishedState is null || !cacheMatchesPublishedInfo,
                currentState,
                shouldPersistState,
                metadataChanged));
        }

        return summaries;
    }

    public async Task PersistStateAsync(
        IEnumerable<DataIntegrationRunSummary> summaries,
        CancellationToken cancellationToken = default)
    {
        foreach (var summary in summaries)
        {
            if (!summary.ShouldPersistState || summary.StateToPersist is null)
                continue;

            await _stateStore.SaveAsync(summary.Descriptor, summary.StateToPersist, cancellationToken);
        }
    }

    public static IReadOnlyList<string> ComputeChangedCnpjs(
        IReadOnlyDictionary<string, string> previousHashes,
        IReadOnlyDictionary<string, string> currentHashes)
    {
        var changed = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var (cnpj, currentHash) in currentHashes)
        {
            if (!previousHashes.TryGetValue(cnpj, out var previousHash)
                || !string.Equals(previousHash, currentHash, StringComparison.Ordinal))
            {
                changed.Add(cnpj);
            }
        }

        foreach (var cnpj in previousHashes.Keys)
        {
            if (!currentHashes.ContainsKey(cnpj))
                changed.Add(cnpj);
        }

        return changed.ToArray();
    }

    private static bool CacheMatchesPublishedInfo(
        DataIntegrationHashState cachedState,
        DataIntegrationPublishedState? publishedState)
    {
        if (publishedState is null)
            return false;

        if (!string.Equals(cachedState.SchemaVersion, publishedState.SchemaVersion, StringComparison.Ordinal))
            return false;

        if (!string.IsNullOrWhiteSpace(publishedState.SourceVersion))
        {
            return string.Equals(
                cachedState.SourceVersion,
                publishedState.SourceVersion,
                StringComparison.Ordinal);
        }

        return true;
    }

    private static bool ParquetGlobExists(string? parquetGlob)
    {
        if (string.IsNullOrWhiteSpace(parquetGlob))
            return false;

        if (!parquetGlob.Contains('*', StringComparison.Ordinal)
            && !parquetGlob.Contains('?', StringComparison.Ordinal))
        {
            return File.Exists(parquetGlob);
        }

        var firstWildcard = parquetGlob.IndexOfAny(['*', '?']);
        var baseDirectoryEnd = parquetGlob.LastIndexOfAny(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            firstWildcard);
        var baseDirectory = baseDirectoryEnd < 0 ? "." : parquetGlob[..baseDirectoryEnd];
        if (!Directory.Exists(baseDirectory))
            return false;

        var searchPattern = parquetGlob.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase)
            ? "*.parquet"
            : "*";

        return Directory.EnumerateFiles(baseDirectory, searchPattern, SearchOption.AllDirectories).Any();
    }
}
