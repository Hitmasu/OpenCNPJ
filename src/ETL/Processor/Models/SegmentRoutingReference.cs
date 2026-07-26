namespace CNPJExporter.Processors.Models;

internal sealed record SegmentRoutingReference(
    string SegmentId,
    ulong Offset,
    uint Length);
