namespace CNPJExporter.Processors.Models;

internal sealed record PublishedZipArtifactSnapshot(
    bool Available,
    long Size,
    string Url,
    string Md5Checksum)
{
    public static PublishedZipArtifactSnapshot Missing { get; } = new(
        false,
        0L,
        string.Empty,
        string.Empty);

    public ZipArtifactPublication ToPublication() =>
        new(
            Available,
            Size,
            Url,
            Md5Checksum);
}
