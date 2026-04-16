namespace CNPJExporter.Modules.Receita.Models;

public sealed record DavEntry(
    string Name,
    Uri Uri,
    bool IsCollection,
    long? ContentLength,
    string? ContentType,
    string? ETag,
    DateTimeOffset? LastModified);
