namespace CNPJExporter.Processors.Models;

internal sealed record ModuleRoutingPublishResult(
    string RoutingReleaseId,
    string LocalRoutingDirectory,
    IReadOnlyList<string> GeneratedPrefixes);
