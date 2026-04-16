namespace CNPJExporter.Integrations;

public sealed record DataIntegrationPublishedState(
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    string? SchemaVersion);
