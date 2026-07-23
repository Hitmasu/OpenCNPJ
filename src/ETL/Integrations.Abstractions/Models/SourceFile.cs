namespace CNPJExporter.Integrations;

public sealed record SourceFile(
    Uri Uri,
    string FileName,
    string SourceVersion,
    long? ContentLength,
    DateTimeOffset? LastModified,
    string? DisplayName = null);
