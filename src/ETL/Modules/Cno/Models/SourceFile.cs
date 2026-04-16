namespace CNPJExporter.Modules.Cno.Models;

public sealed record SourceFile(
    Uri Uri,
    string FileName,
    string SourceVersion,
    long? ContentLength,
    DateTimeOffset? LastModified);
