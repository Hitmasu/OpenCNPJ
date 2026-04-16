namespace CNPJExporter.Processors.Models;

internal sealed record ShardReleasePlan(
    IReadOnlyList<string> PrefixesUploadOnly,
    IReadOnlyList<string> PrefixesToGenerate);
