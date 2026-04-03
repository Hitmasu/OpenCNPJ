namespace CNPJExporter.Utils;

public static class DirectoryUtils
{
    public static async Task RecreateDirectoryAsync(string path)
    {
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
            return;
        }

        var fullPath = Path.GetFullPath(path);
        var parentDirectory = Path.GetDirectoryName(fullPath);
        if (string.IsNullOrWhiteSpace(parentDirectory))
            throw new IOException($"Não foi possível resolver o diretório pai de '{path}'.");

        Exception? lastError = null;

        for (var attempt = 1; attempt <= 5; attempt++)
        {
            string? backupPath = null;

            try
            {
                if (Directory.Exists(fullPath))
                {
                    backupPath = Path.Combine(
                        parentDirectory,
                        $".recreate-{Path.GetFileName(fullPath)}-{Guid.NewGuid():N}");

                    Directory.Move(fullPath, backupPath);
                }

                Directory.CreateDirectory(fullPath);

                if (!string.IsNullOrEmpty(backupPath))
                    await TryDeleteDirectoryBestEffortAsync(backupPath);

                return;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                lastError = ex;
                await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt));
            }
        }

        throw new IOException($"Falha ao recriar diretório '{path}'.", lastError);
    }

    private static async Task TryDeleteDirectoryBestEffortAsync(string path)
    {
        Exception? lastError = null;

        for (var attempt = 1; attempt <= 5; attempt++)
        {
            try
            {
                DeleteDirectoryTree(path);
                return;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                lastError = ex;
                await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt));
            }
        }

        if (lastError is not null)
        {
            // O diretório novo já foi criado; falha de cleanup não deve bloquear o pipeline.
            return;
        }
    }

    private static void DeleteDirectoryTree(string path)
    {
        if (!Directory.Exists(path))
            return;

        foreach (var file in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
        {
            File.SetAttributes(file, FileAttributes.Normal);
        }

        Directory.Delete(path, true);
    }
}
