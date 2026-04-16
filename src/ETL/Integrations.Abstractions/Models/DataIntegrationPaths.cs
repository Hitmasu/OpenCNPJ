namespace CNPJExporter.Integrations;

public sealed record DataIntegrationPaths(
    string DataDir,
    string ParquetDir,
    string OutputDir,
    string DownloadDir);
