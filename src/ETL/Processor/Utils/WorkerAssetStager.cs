using CNPJExporter.Configuration;

namespace CNPJExporter.Utils;

public static class WorkerAssetStager
{
    private const string PublicRoot = "files";
    private const string IndexPattern = "*.index.json";

    public static async Task<string> StageAsync(string datasetKey)
    {
        var workerAssetsDir = AppConfig.Current.Paths.WorkerAssetsDir;
        if (string.IsNullOrWhiteSpace(workerAssetsDir))
            throw new InvalidOperationException("Paths.WorkerAssetsDir não foi configurado.");

        var datasetOutputDir = DatasetPathResolver.GetDatasetPath(AppConfig.Current.Paths.OutputDir, datasetKey);
        var sourceShardDir = Path.Combine(datasetOutputDir, AppConfig.Current.Shards.RemoteDir);
        var sourceInfoPath = Path.Combine(datasetOutputDir, "info.json");

        if (!Directory.Exists(sourceShardDir))
            throw new DirectoryNotFoundException($"Diretório de shards não encontrado: {sourceShardDir}");

        if (!File.Exists(sourceInfoPath))
            throw new FileNotFoundException("info.json local não encontrado para staging do Worker.", sourceInfoPath);

        var targetRoot = Path.GetFullPath(workerAssetsDir);
        var targetPublicRoot = Path.Combine(targetRoot, PublicRoot);
        var targetShardDir = Path.Combine(targetPublicRoot, AppConfig.Current.Shards.RemoteDir);

        await DirectoryUtils.RecreateDirectoryAsync(targetRoot);
        Directory.CreateDirectory(targetShardDir);

        await CopyFileAsync(sourceInfoPath, Path.Combine(targetPublicRoot, "info.json"));

        foreach (var indexPath in Directory.EnumerateFiles(sourceShardDir, IndexPattern, SearchOption.TopDirectoryOnly))
        {
            await CopyFileAsync(indexPath, Path.Combine(targetShardDir, Path.GetFileName(indexPath)));
        }

        return targetRoot;
    }

    private static async Task CopyFileAsync(string sourcePath, string destinationPath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(destinationPath)!);

        await using var source = File.Open(sourcePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        await using var destination = File.Open(destinationPath, FileMode.Create, FileAccess.Write, FileShare.None);
        await source.CopyToAsync(destination);
    }
}
