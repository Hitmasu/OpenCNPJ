namespace CNPJExporter.Integrations;

public sealed record DataIntegrationRunContext(
    string DatasetKey,
    DataIntegrationPaths Paths,
    string ModuleWorkDir,
    string ModuleParquetDir,
    DataIntegrationHashState PreviousState,
    DateTimeOffset Now);
