namespace CNPJExporter.Processors.Models;

internal sealed class SparseShardIndexEntry
{
    public required string Cnpj { get; init; }

    public required long Offset { get; init; }
}
