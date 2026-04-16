using CNPJExporter.Configuration;

namespace CNPJExporter.Utils;

public static class LocalArtifactCleaner
{
    public static async Task CleanupDatasetArtifactsAsync(string datasetKey)
    {
        if (!DatasetPathResolver.IsDatasetKey(datasetKey))
            throw new ArgumentException("Dataset inválido para cleanup.", nameof(datasetKey));

        var paths = AppConfig.Current.Paths;
        var datasetDirectories = new[]
        {
            DatasetPathResolver.GetDatasetPath(paths.DownloadDir, datasetKey),
            DatasetPathResolver.GetDatasetPath(paths.DataDir, datasetKey)
        };

        foreach (var directory in datasetDirectories.Distinct(StringComparer.Ordinal))
        {
            await DirectoryUtils.DeleteDirectoryIfExistsAsync(directory);
        }

        await CleanupIntegrationInputArtifactsAsync(paths.DataDir);

        if (!AppConfig.Current.DuckDb.UseInMemory)
        {
            var duckDbPath = Path.GetFullPath("cnpj.duckdb");
            if (File.Exists(duckDbPath))
                File.Delete(duckDbPath);
        }

        await DirectoryUtils.DeleteDirectoryIfExistsAsync(Path.GetFullPath("temp"));
    }

    private static async Task CleanupIntegrationInputArtifactsAsync(string dataDir)
    {
        var integrationsDir = Path.Combine(dataDir, "integrations");
        if (!Directory.Exists(integrationsDir))
            return;

        foreach (var directory in Directory.EnumerateDirectories(integrationsDir))
        {
            if (string.Equals(Path.GetFileName(directory), "_state", StringComparison.Ordinal))
                continue;

            await DirectoryUtils.DeleteDirectoryIfExistsAsync(directory);
        }

        foreach (var file in Directory.EnumerateFiles(integrationsDir))
        {
            File.Delete(file);
        }
    }
}
