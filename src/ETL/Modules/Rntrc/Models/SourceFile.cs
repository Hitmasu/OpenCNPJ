namespace CNPJExporter.Modules.Rntrc.Models;

public sealed record SourceFile(
    Uri Uri,
    string DisplayName,
    string FileName,
    string SourceVersion,
    long? ContentLength,
    DateTimeOffset? LastModified);
