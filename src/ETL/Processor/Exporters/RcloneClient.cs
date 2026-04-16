using System.Diagnostics;
using System.Text.RegularExpressions;
using CNPJExporter.Configuration;
using Spectre.Console;

namespace CNPJExporter.Exporters;

public static class RcloneClient
{
    private static readonly SemaphoreSlim UploadSemaphore = new(AppConfig.Current.Rclone.MaxConcurrentUploads);
    
    private static readonly Regex TransferRegex = new(@"Transferred:\s+\d+\s*/\s*\d+,\s*(\d+)%", RegexOptions.Compiled);
    
    private static string RemoteBase =>
        (Environment.GetEnvironmentVariable("RCLONE_REMOTE") ?? AppConfig.Current.Rclone.RemoteBase).TrimEnd('/');

    private static int Transfers => Math.Max(1, AppConfig.Current.Rclone.Transfers);
    private static string BufferSize => NormalizeBufferSize(AppConfig.Current.Rclone.BufferSize);
    private static int UploadVerificationRetries => Math.Max(1, AppConfig.Current.Rclone.UploadVerificationRetries);
    private static TimeSpan UploadVerificationDelay => TimeSpan.FromSeconds(Math.Max(1, AppConfig.Current.Rclone.UploadVerificationDelaySeconds));

    public static async Task<bool> UploadFolderAsync(
        string localFolderPath,
        string? remoteRelativePath = null,
        ProgressTask? progressTask = null,
        params string[] includePatterns)
    {
        await UploadSemaphore.WaitAsync();
        try
        {
            var remote = string.IsNullOrWhiteSpace(remoteRelativePath)
                ? RemoteBase + "/"
                : RemoteBase + "/" + remoteRelativePath.Trim('/');
            var filterArgs = BuildFilterArguments(includePatterns);
            var localFileCount = CountLocalFiles(localFolderPath, includePatterns);

            for (var attempt = 1; attempt <= UploadVerificationRetries; attempt++)
            {
                var command = $"copy \"{localFolderPath}\" \"{remote}\" " +
                              filterArgs +
                              $"--progress --stats=1s --transfers={Transfers} " +
                              $"--checksum --fast-list=false " +
                              $"--no-update-modtime " +
                              $"--buffer-size={BufferSize} --checkers=1 " +
                              $"--bwlimit=off " +
                              $"--retries=-1 --retries-sleep=60s --low-level-retries=10";

                var result = await RunRcloneCommandAsync(command, progressTask);
                var remoteFileCount = await CountRemoteFilesAsync(remote, includePatterns);
                var uploadComplete = IsUploadComplete(localFileCount, remoteFileCount);

                if (result.ExitCode == 0 && uploadComplete)
                    return true;

                if (uploadComplete)
                {
                    AnsiConsole.MarkupLine("[yellow]⚠️ rclone reportou erro transitório, mas o release remoto ficou completo. Prosseguindo.[/]");
                    return true;
                }

                if (!string.IsNullOrWhiteSpace(result.Error))
                {
                    AnsiConsole.MarkupLine($"[red]Erro no rclone upload (tentativa {attempt}/{UploadVerificationRetries}): {result.Error.EscapeMarkup()}[/]");
                }

                if (attempt < UploadVerificationRetries)
                {
                    AnsiConsole.MarkupLine($"[yellow]Tentando novamente o upload do release em {UploadVerificationDelay.TotalSeconds:0}s...[/]");
                    await Task.Delay(UploadVerificationDelay);
                }
            }

            return false;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Erro no rclone upload: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
        finally
        {
            UploadSemaphore.Release();
        }
    }

    public static async Task<bool> UploadSelectedFilesAsync(
        string localFolderPath,
        string remoteRelativePath,
        IReadOnlyCollection<string> relativeFiles,
        ProgressTask? progressTask = null)
    {
        if (relativeFiles.Count == 0)
            return true;

        await UploadSemaphore.WaitAsync();
        try
        {
            var remote = RemoteBase + "/" + remoteRelativePath.Trim('/');
            var filesFromPath = Path.Combine(Path.GetTempPath(), $"opencnpj-rclone-files-{Guid.NewGuid():N}.txt");
            await File.WriteAllLinesAsync(filesFromPath, relativeFiles.OrderBy(path => path, StringComparer.Ordinal));

            try
            {
                for (var attempt = 1; attempt <= UploadVerificationRetries; attempt++)
                {
                    var command = $"copy \"{localFolderPath}\" \"{remote}\" " +
                                  BuildFilesFromArgument(filesFromPath) +
                                  $"--progress --stats=1s --transfers={Transfers} " +
                                  $"--checksum --fast-list=false " +
                                  $"--no-update-modtime " +
                                  $"--buffer-size={BufferSize} --checkers=1 " +
                                  $"--bwlimit=off " +
                                  $"--retries=-1 --retries-sleep=60s --low-level-retries=10";

                    var result = await RunRcloneCommandAsync(command, progressTask);
                    var remoteHashes = await ListRemoteChecksumsAsync(remoteRelativePath, ["*.ndjson", "*.index.bin"]);
                    var remaining = relativeFiles
                        .Where(path => path.EndsWith(".ndjson", StringComparison.OrdinalIgnoreCase)
                                       || path.EndsWith(".index.bin", StringComparison.OrdinalIgnoreCase))
                        .Count(path => !remoteHashes.ContainsKey(path));

                    if (result.ExitCode == 0 && remaining == 0)
                        return true;

                    if (remaining == 0)
                    {
                        AnsiConsole.MarkupLine("[yellow]⚠️ rclone reportou erro transitório, mas todos os arquivos selecionados chegaram ao destino. Prosseguindo.[/]");
                        return true;
                    }

                    if (!string.IsNullOrWhiteSpace(result.Error))
                    {
                        AnsiConsole.MarkupLine($"[red]Erro no upload seletivo (tentativa {attempt}/{UploadVerificationRetries}): {result.Error.EscapeMarkup()}[/]");
                    }
                    else
                    {
                        AnsiConsole.MarkupLine(
                            $"[yellow]Upload seletivo incompleto (tentativa {attempt}/{UploadVerificationRetries}): exit={result.ExitCode}, pendentes={remaining}[/]");
                    }

                    if (attempt < UploadVerificationRetries)
                    {
                        AnsiConsole.MarkupLine($"[yellow]Tentando novamente o upload seletivo em {UploadVerificationDelay.TotalSeconds:0}s...[/]");
                        await Task.Delay(UploadVerificationDelay);
                    }
                }

                return false;
            }
            finally
            {
                DeleteIfExists(filesFromPath);
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Erro no rclone upload seletivo: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
        finally
        {
            UploadSemaphore.Release();
        }
    }

    public static async Task<Dictionary<string, string>> ListRemoteChecksumsAsync(
        string remoteRelativePath,
        params string[] includePatterns)
    {
        var remote = RemoteBase + "/" + remoteRelativePath.Trim('/');
        var result = await RunRcloneCommandAsync(
            BuildRemoteMd5SumArguments(remote, includePatterns));

        if (result.ExitCode != 0)
            return new Dictionary<string, string>(StringComparer.Ordinal);

        return ParseMd5SumOutput(result.Output);
    }

    internal static string BuildFilterArgumentsForTest(IEnumerable<string> includePatterns) => BuildFilterArguments(includePatterns);

    internal static bool IsUploadCompleteForTest(int localFileCount, int remoteFileCount) =>
        IsUploadComplete(localFileCount, remoteFileCount);

    internal static string BuildRemoteMd5SumArgumentsForTest(
        string remotePath,
        IEnumerable<string> includePatterns) =>
        BuildRemoteMd5SumArguments(remotePath, includePatterns);

    internal static string NormalizeBufferSizeForTest(string? bufferSize) => NormalizeBufferSize(bufferSize);

    internal static string BuildFilesFromArgumentForTest(string filesFromPath) => BuildFilesFromArgument(filesFromPath);

    private static string NormalizeBufferSize(string? bufferSize) =>
        string.IsNullOrWhiteSpace(bufferSize) ? "16M" : bufferSize.Trim();

    private static string BuildFilesFromArgument(string filesFromPath) =>
        $"--files-from-raw \"{filesFromPath}\" ";

    private static string BuildFilterArguments(IEnumerable<string> includePatterns)
    {
        var normalizedPatterns = includePatterns
            .Where(pattern => !string.IsNullOrWhiteSpace(pattern))
            .Select(pattern => pattern.Trim())
            .ToArray();

        if (normalizedPatterns.Length == 0)
            return string.Empty;

        var args = normalizedPatterns
            .Select(pattern => $"--filter \"+ {pattern.Replace("\"", "\\\"")}\"")
            .Append("--filter \"- **\"");

        return string.Join(" ", args) + " ";
    }

    private static string BuildRemoteMd5SumArguments(string remotePath, IEnumerable<string> includePatterns)
    {
        var filterArgs = BuildFilterArguments(includePatterns);
        return $"md5sum \"{remotePath}\" {filterArgs}";
    }

    private static bool IsUploadComplete(int localFileCount, int remoteFileCount) =>
        localFileCount >= 0 && remoteFileCount >= 0 && remoteFileCount == localFileCount;

    private static int CountLocalFiles(string localFolderPath, IEnumerable<string> includePatterns)
    {
        var patterns = includePatterns
            .Where(pattern => !string.IsNullOrWhiteSpace(pattern))
            .Select(BuildGlobRegex)
            .ToArray();

        return Directory
            .EnumerateFiles(localFolderPath, "*", SearchOption.AllDirectories)
            .Select(path => Path.GetRelativePath(localFolderPath, path).Replace('\\', '/'))
            .Count(path => patterns.Length == 0 || patterns.Any(regex => regex.IsMatch(path)));
    }

    private static Dictionary<string, string> ParseMd5SumOutput(string output)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var line in output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var parts = line.Split("  ", 2, StringSplitOptions.None);
            if (parts.Length != 2)
                continue;

            var hash = parts[0].Trim();
            var path = NormalizeRelativePath(parts[1]);
            if (string.IsNullOrWhiteSpace(hash) || string.IsNullOrWhiteSpace(path))
                continue;

            result[path] = hash.ToLowerInvariant();
        }

        return result;
    }

    private static string NormalizeRelativePath(string path) =>
        path.Replace('\\', '/').TrimStart('/');

    private static async Task<int> CountRemoteFilesAsync(string remotePath, IEnumerable<string> includePatterns)
    {
        var filterArgs = BuildFilterArguments(includePatterns);
        var result = await RunRcloneCommandAsync($"lsf \"{remotePath}\" {filterArgs}--files-only --recursive");
        if (result.ExitCode != 0)
            return -1;

        return result.Output
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Length;
    }

    private static Regex BuildGlobRegex(string pattern)
    {
        var escaped = Regex.Escape(pattern.Trim())
            .Replace("\\*\\*", ".*")
            .Replace("\\*", "[^/]*")
            .Replace("\\?", "[^/]");

        return new Regex($"^{escaped}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    }

    private static async Task<RcloneCommandResult> RunRcloneCommandAsync(string arguments, ProgressTask? progressTask = null)
    {
        using var process = new Process();
        process.StartInfo.FileName = "rclone";
        process.StartInfo.Arguments = arguments;
        process.StartInfo.UseShellExecute = false;
        process.StartInfo.RedirectStandardOutput = true;
        process.StartInfo.RedirectStandardError = true;
        process.EnableRaisingEvents = true;

        var outputBuffer = new System.Text.StringBuilder();
        var errorBuffer = new System.Text.StringBuilder();

        process.OutputDataReceived += (_, e) =>
        {
            if (string.IsNullOrEmpty(e.Data)) return;
            outputBuffer.AppendLine(e.Data);
            var match = TransferRegex.Match(e.Data);
            if (match.Success && progressTask != null && int.TryParse(match.Groups[1].Value, out var percentage))
            {
                progressTask.Value = percentage;
                progressTask.Description = $"[cyan]Upload: {percentage}%[/]";
            }
        };

        process.ErrorDataReceived += (_, e) =>
        {
            if (string.IsNullOrEmpty(e.Data)) return;
            errorBuffer.AppendLine(e.Data);
            if (e.Data.Contains("ERROR", StringComparison.OrdinalIgnoreCase))
            {
                AnsiConsole.MarkupLine($"[red]rclone: {e.Data.EscapeMarkup()}[/]");
            }
        };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        await process.WaitForExitAsync();

        return new RcloneCommandResult(process.ExitCode, outputBuffer.ToString(), errorBuffer.ToString());
    }

    public static async Task<bool> DownloadFileAsync(string remoteRelativePath, string localFilePath)
    {
        var remote = RemoteBase + "/" + remoteRelativePath.TrimStart('/');
        return await CopyToAsync(remote, localFilePath);
    }


    public static async Task<bool> CopyToAsync(string remotePath, string localFilePath)
    {
        try
        {
            var result = await RunRcloneCommandAsync(
                $"copyto \"{remotePath}\" \"{localFilePath}\" --checksum --retries=-1 --retries-sleep=60s --low-level-retries=10 --bwlimit=off");

            var ok = result.ExitCode == 0 && File.Exists(localFilePath);
            if (!ok && !string.IsNullOrWhiteSpace(result.Error))
            {
                AnsiConsole.MarkupLine($"[yellow]⚠️ rclone copyto falhou: {result.Error.EscapeMarkup()}[/]");
            }
            return ok;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[yellow]⚠️ Erro no rclone copyto: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
    }

    public static async Task<bool> UploadFileAsync(string localFilePath, string remoteRelativePath)
    {
        try
        {
            var remotePath = RemoteBase + "/" + remoteRelativePath.TrimStart('/');
            var result = await RunRcloneCommandAsync(
                $"copyto \"{localFilePath}\" \"{remotePath}\" --checksum --retries=-1 --retries-sleep=60s --low-level-retries=10 --bwlimit=off --no-update-modtime");

            var ok = result.ExitCode == 0;
            if (!ok && !string.IsNullOrWhiteSpace(result.Error))
            {
                AnsiConsole.MarkupLine($"[yellow]⚠️ rclone upload file falhou: {result.Error.EscapeMarkup()}[/]");
            }
            return ok;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[yellow]⚠️ Erro no rclone upload file: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
    }

    private sealed record RcloneCommandResult(int ExitCode, string Output, string Error);

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
