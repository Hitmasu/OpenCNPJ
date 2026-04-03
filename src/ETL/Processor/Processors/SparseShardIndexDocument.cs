namespace CNPJExporter.Processors;

internal sealed class SparseShardIndexDocument
{
    public int Version { get; init; } = 1;

    public required string Format { get; init; }

    public required string Prefix { get; init; }

    public required string DataFile { get; init; }

    public required int Stride { get; init; }

    public required int RecordCount { get; init; }

    public required long DataSize { get; init; }

    public required List<SparseShardIndexEntry> Entries { get; init; }
}
