namespace CNPJExporter.Processors.Models;

internal sealed record ZipArtifactPublication(
    bool Available,
    long Size,
    string Url,
    string Md5Checksum)
{
    public static ZipArtifactPublication Missing { get; } = new(
        false,
        0L,
        string.Empty,
        string.Empty);
}
