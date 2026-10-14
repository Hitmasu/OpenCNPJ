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

    private static string RcloneExecutable => ResolveExecutable(AppConfig.Current.Rclone.Executable);
    private static int Transfers => Math.Max(1, AppConfig.Current.Rclone.Transfers);
    private static string BufferSize => NormalizeBufferSize(AppConfig.Current.Rclone.BufferSize);
    private static int UploadVerificationRetries => Math.Max(1, AppConfig.Current.Rclone.UploadVerificationRetries);
    private static TimeSpan UploadVerificationDelay => TimeSpan.FromSeconds(Math.Max(1, AppConfig.Current.Rclone.UploadVerificationDelaySeconds));
    private static TimeSpan CommandTimeout => ResolveCommandTimeout(AppConfig.Current.Rclone.CommandTimeoutSeconds);

    public static Task<bool> UploadFolderAsync(
        string localFolderPath,
        string? remoteRelativePath = null,
        ProgressTask? progressTask = null,
        params string[] includePatterns) =>
        UploadFolderCoreAsync(localFolderPath, remoteRelativePath, progressTask, CancellationToken.None, includePatterns);

    public static Task<bool> UploadFolderAsync(
        CancellationToken cancellationToken,
        string localFolderPath,
        string? remoteRelativePath = null,
        ProgressTask? progressTask = null,
        params string[] includePatterns) =>
        UploadFolderCoreAsync(localFolderPath, remoteRelativePath, progressTask, cancellationToken, includePatterns);

    private static async Task<bool> UploadFolderCoreAsync(
        string localFolderPath,
        string? remoteRelativePath,
        ProgressTask? progressTask,
        CancellationToken cancellationToken,
        params string[] includePatterns)
    {
        await UploadSemaphore.WaitAsync(cancellationToken);
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

                var result = await RunRcloneCommandAsync(command, progressTask, cancellationToken);
                var remoteFileCount = await CountRemoteFilesAsync(remote, includePatterns, cancellationToken);
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
                    await Task.Delay(UploadVerificationDelay, cancellationToken);
                }
            }

            return false;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
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
        ProgressTask? progressTask = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (relativeFiles.Count == 0)
            return true;

        await UploadSemaphore.WaitAsync(cancellationToken);
        try
        {
            var remote = RemoteBase + "/" + remoteRelativePath.Trim('/');
            var filesFromPath = Path.Combine(Path.GetTempPath(), $"opencnpj-rclone-files-{Guid.NewGuid():N}.txt");
            var selectedFiles = BuildSelectedUploadFiles(relativeFiles);
            await File.WriteAllLinesAsync(filesFromPath, selectedFiles, cancellationToken);

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

                    var result = await RunRcloneCommandAsync(command, progressTask, cancellationToken);
                    var remoteHashes = await ListSelectedRemoteChecksumsAsync(remoteRelativePath, filesFromPath, cancellationToken);
                    var remaining = selectedFiles.Count(path => !remoteHashes.ContainsKey(NormalizeRelativePath(path)));

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
                        await Task.Delay(UploadVerificationDelay, cancellationToken);
                    }
                }

                return false;
            }
            finally
            {
                DeleteIfExists(filesFromPath);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
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

    public static Task<Dictionary<string, string>> ListRemoteChecksumsAsync(
        string remoteRelativePath,
        params string[] includePatterns) =>
        ListRemoteChecksumsCoreAsync(remoteRelativePath, CancellationToken.None, includePatterns);

    public static Task<Dictionary<string, string>> ListRemoteChecksumsAsync(
        CancellationToken cancellationToken,
        string remoteRelativePath,
        params string[] includePatterns) =>
        ListRemoteChecksumsCoreAsync(remoteRelativePath, cancellationToken, includePatterns);

    private static async Task<Dictionary<string, string>> ListRemoteChecksumsCoreAsync(
        string remoteRelativePath,
        CancellationToken cancellationToken,
        params string[] includePatterns)
    {
        var remote = RemoteBase + "/" + remoteRelativePath.Trim('/');
        var result = await RunRcloneCommandAsync(
            BuildRemoteMd5SumArguments(remote, includePatterns), cancellationToken: cancellationToken);

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

    internal static string BuildSelectedRemoteMd5SumArgumentsForTest(
        string remotePath,
        string filesFromPath) =>
        BuildSelectedRemoteMd5SumArguments(remotePath, filesFromPath);

    internal static string NormalizeBufferSizeForTest(string? bufferSize) => NormalizeBufferSize(bufferSize);

    internal static string BuildFilesFromArgumentForTest(string filesFromPath) => BuildFilesFromArgument(filesFromPath);

    internal static IReadOnlyList<string> BuildSelectedUploadFilesForTest(IEnumerable<string> relativeFiles) =>
        BuildSelectedUploadFiles(relativeFiles);

    internal static string ResolveExecutableForTest(string? executable) => ResolveExecutable(executable);

    internal static TimeSpan ResolveCommandTimeoutForTest(int seconds) => ResolveCommandTimeout(seconds);

    private static TimeSpan ResolveCommandTimeout(int seconds)
    {
        if (seconds <= 0)
            throw new ArgumentOutOfRangeException(nameof(seconds), "Rclone.CommandTimeoutSeconds deve ser maior que zero.");

        return TimeSpan.FromSeconds(seconds);
    }

    private static string ResolveExecutable(string? executable) =>
        string.IsNullOrWhiteSpace(executable) ? "rclone" : executable.Trim();

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

    private static string BuildSelectedRemoteMd5SumArguments(string remotePath, string filesFromPath) =>
        $"md5sum \"{remotePath}\" {BuildFilesFromArgument(filesFromPath)}";

    private static string[] BuildSelectedUploadFiles(IEnumerable<string> relativeFiles) =>
        relativeFiles.OrderBy(path => path, StringComparer.Ordinal).ToArray();

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

    private static async Task<Dictionary<string, string>> ListSelectedRemoteChecksumsAsync(
        string remoteRelativePath,
        string filesFromPath,
        CancellationToken cancellationToken)
    {
        var remote = RemoteBase + "/" + remoteRelativePath.Trim('/');
        var result = await RunRcloneCommandAsync(
            BuildSelectedRemoteMd5SumArguments(remote, filesFromPath), cancellationToken: cancellationToken);

        if (result.ExitCode != 0)
            return new Dictionary<string, string>(StringComparer.Ordinal);

        return ParseMd5SumOutput(result.Output);
    }

    private static async Task<int> CountRemoteFilesAsync(
        string remotePath,
        IEnumerable<string> includePatterns,
        CancellationToken cancellationToken)
    {
        var filterArgs = BuildFilterArguments(includePatterns);
        var result = await RunRcloneCommandAsync($"lsf \"{remotePath}\" {filterArgs}--files-only --recursive", cancellationToken: cancellationToken);
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

    // A single timeout applies to each rclone subprocess, including remote verification commands.
    private static Task<RcloneCommandResult> RunRcloneCommandAsync(
        string arguments,
        ProgressTask? progressTask = null,
        CancellationToken cancellationToken = default) =>
        RunProcessAsync(RcloneExecutable, arguments, CommandTimeout, progressTask, cancellationToken);

    internal static Task<RcloneCommandResult> RunRcloneCommandForTestAsync(
        string executable,
        string arguments,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        RunProcessAsync(executable, arguments, timeout, null, cancellationToken);

    private static async Task<RcloneCommandResult> RunProcessAsync(
        string executable,
        string arguments,
        TimeSpan timeout,
        ProgressTask? progressTask,
        CancellationToken cancellationToken)
    {
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "O timeout do rclone deve ser maior que zero.");

        cancellationToken.ThrowIfCancellationRequested();

        using var process = new Process();
        process.StartInfo.FileName = executable;
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

        using var timeoutSource = new CancellationTokenSource(timeout);
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);

        try
        {
            await process.WaitForExitAsync(linkedSource.Token);
        }
        catch (OperationCanceledException) when (linkedSource.IsCancellationRequested)
        {
            // Canceling WaitForExitAsync alone does not stop the child process.
            await TerminateProcessTreeAsync(process);
            cancellationToken.ThrowIfCancellationRequested();

            var message = $"Comando rclone excedeu o timeout de {timeout.TotalSeconds:0} segundos.";
            var standardError = errorBuffer.ToString();
            if (!string.IsNullOrWhiteSpace(standardError))
                message += Environment.NewLine + standardError;

            AnsiConsole.MarkupLine($"[yellow]{message.EscapeMarkup()}[/]");
            return new RcloneCommandResult(-1, outputBuffer.ToString(), message, TimedOut: true);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return new RcloneCommandResult(process.ExitCode, outputBuffer.ToString(), errorBuffer.ToString());
    }

    private static async Task TerminateProcessTreeAsync(Process process)
    {
        try
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // The process may have exited between HasExited and Kill.
        }

        // Bound cleanup as well; a descendant may retain redirected pipe handles.
        await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
    }

    public static Task<bool> DownloadFileAsync(
        string remoteRelativePath,
        string localFilePath,
        CancellationToken cancellationToken = default)
    {
        var remote = RemoteBase + "/" + remoteRelativePath.TrimStart('/');
        return CopyToAsync(remote, localFilePath, cancellationToken);
    }

    public static async Task<bool> DownloadFolderAsync(
        string remoteRelativePath,
        string localFolderPath,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            Directory.CreateDirectory(localFolderPath);
            var remote = RemoteBase + "/" + remoteRelativePath.Trim('/');
            var result = await RunRcloneCommandAsync(
                $"copy \"{remote}\" \"{localFolderPath}\" --checksum --retries=-1 --retries-sleep=60s --low-level-retries=10 --bwlimit=off", cancellationToken: cancellationToken);
            if (result.ExitCode == 0)
                return true;

            if (!string.IsNullOrWhiteSpace(result.Error))
            {
                AnsiConsole.MarkupLine(
                    $"[yellow]⚠️ rclone download de diretório falhou: {result.Error.EscapeMarkup()}[/]");
            }

            return false;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine(
                $"[yellow]⚠️ Erro no download de diretório via rclone: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
    }

    public static async Task<bool> CopyToAsync(
        string remotePath,
        string localFilePath,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            var result = await RunRcloneCommandAsync(
                $"copyto \"{remotePath}\" \"{localFilePath}\" --checksum --retries=-1 --retries-sleep=60s --low-level-retries=10 --bwlimit=off", cancellationToken: cancellationToken);

            var ok = result.ExitCode == 0 && File.Exists(localFilePath);
            if (!ok && !string.IsNullOrWhiteSpace(result.Error))
            {
                AnsiConsole.MarkupLine($"[yellow]⚠️ rclone copyto falhou: {result.Error.EscapeMarkup()}[/]");
            }
            return ok;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[yellow]⚠️ Erro no rclone copyto: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
    }

    public static async Task<bool> UploadFileAsync(
        string localFilePath,
        string remoteRelativePath,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            var remotePath = RemoteBase + "/" + remoteRelativePath.TrimStart('/');
            var result = await RunRcloneCommandAsync(
                $"copyto \"{localFilePath}\" \"{remotePath}\" --checksum --retries=-1 --retries-sleep=60s --low-level-retries=10 --bwlimit=off --no-update-modtime", cancellationToken: cancellationToken);

            var ok = result.ExitCode == 0;
            if (!ok && !string.IsNullOrWhiteSpace(result.Error))
            {
                AnsiConsole.MarkupLine($"[yellow]⚠️ rclone upload file falhou: {result.Error.EscapeMarkup()}[/]");
            }
            return ok;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[yellow]⚠️ Erro no rclone upload file: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
    }

    internal sealed record RcloneCommandResult(int ExitCode, string Output, string Error, bool TimedOut = false);

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
