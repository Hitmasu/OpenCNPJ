namespace CNPJExporter.Processors.Models;

internal sealed record ModuleSegmentPublication(
    string Id,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    long RecordCount,
    string StorageReleaseId,
    ZipArtifactPublication Zip);
