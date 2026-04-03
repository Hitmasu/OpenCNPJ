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
            DatasetPathResolver.GetDatasetPath(paths.DataDir, datasetKey),
            DatasetPathResolver.GetDatasetPath(paths.ParquetDir, datasetKey),
            DatasetPathResolver.GetDatasetPath(paths.OutputDir, datasetKey)
        };

        foreach (var directory in datasetDirectories.Distinct(StringComparer.Ordinal))
        {
            await DirectoryUtils.DeleteDirectoryIfExistsAsync(directory);
        }

        if (!AppConfig.Current.DuckDb.UseInMemory)
        {
            var duckDbPath = Path.GetFullPath("cnpj.duckdb");
            if (File.Exists(duckDbPath))
                File.Delete(duckDbPath);
        }

        await DirectoryUtils.DeleteDirectoryIfExistsAsync(Path.GetFullPath("hash_cache"));
        await DirectoryUtils.DeleteDirectoryIfExistsAsync(Path.GetFullPath("temp"));
    }
}
