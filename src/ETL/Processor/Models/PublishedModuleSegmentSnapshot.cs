namespace CNPJExporter.Processors.Models;

internal sealed record PublishedModuleSegmentSnapshot(
    string Id,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    long RecordCount,
    string StorageReleaseId,
    PublishedZipArtifactSnapshot Zip)
{
    public ModuleSegmentPublication ToPublication() =>
        new(
            Id,
            SourceVersion,
            UpdatedAt,
            RecordCount,
            StorageReleaseId,
            Zip.ToPublication());
}
