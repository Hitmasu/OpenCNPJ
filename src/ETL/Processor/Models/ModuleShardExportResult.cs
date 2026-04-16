namespace CNPJExporter.Processors.Models;

internal sealed record ModuleShardExportResult(
    string LocalShardDir,
    IReadOnlyList<string> GeneratedPrefixes);
