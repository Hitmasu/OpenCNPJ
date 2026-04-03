using System.Text.RegularExpressions;
using CNPJExporter.Configuration;

namespace CNPJExporter.Utils;

public static class DatasetPathResolver
{
    private static readonly Regex DatasetPattern = new(@"^\d{4}-\d{2}$", RegexOptions.Compiled);

    public static bool IsDatasetKey(string? value)
    {
        return !string.IsNullOrWhiteSpace(value) && DatasetPattern.IsMatch(value);
    }

    public static string GetDatasetPath(string rootDir, string? datasetKey)
    {
        if (!IsDatasetKey(datasetKey))
            return rootDir;

        return Path.Combine(rootDir, datasetKey!);
    }

    public static string? ResolveLatestLocalDatasetKey(params string[] rootDirs)
    {
        var datasetKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var rootDir in rootDirs.Where(Directory.Exists))
        {
            foreach (var directory in Directory.EnumerateDirectories(rootDir))
            {
                var name = Path.GetFileName(directory);
                if (IsDatasetKey(name))
                    datasetKeys.Add(name);
            }
        }

        return datasetKeys.OrderBy(x => x, StringComparer.Ordinal).LastOrDefault();
    }

    public static string? ResolveLatestLocalDatasetKey(AppConfig.PathsConfig paths)
    {
        return ResolveLatestLocalDatasetKey(paths.DownloadDir, paths.DataDir, paths.ParquetDir, paths.OutputDir);
    }
}
