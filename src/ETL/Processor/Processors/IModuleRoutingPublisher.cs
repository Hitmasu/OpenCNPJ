namespace CNPJExporter.Processors;

internal interface IModuleRoutingPublisher
{
    Task<Models.ModuleRoutingPublishResult> PublishAsync(
        string moduleKey,
        string releaseId,
        string? previousRoutingReleaseId,
        IReadOnlyDictionary<string, string> changedSegmentShardDirectories,
        IReadOnlyCollection<string> removedSegmentIds,
        string outputRootDir,
        CancellationToken cancellationToken = default);
}
