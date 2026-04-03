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

    public static async Task<bool> UploadFolderAsync(string localFolderPath, string? remoteRelativePath = null, ProgressTask? progressTask = null)
    {
        await UploadSemaphore.WaitAsync();
        try
        {
            var remote = string.IsNullOrWhiteSpace(remoteRelativePath)
                ? RemoteBase + "/"
                : RemoteBase + "/" + remoteRelativePath.Trim('/');

            using var process = new Process();
            process.StartInfo.FileName = "rclone";
            process.StartInfo.Arguments =
                $"copy \"{localFolderPath}\" \"{remote}\" " +
                $"--progress --stats=1s --transfers={Transfers} " +
                $"--checksum --fast-list=false " +
                $"--no-update-modtime " +
                $"--buffer-size=128M --checkers=1 " +
                $"--bwlimit=off " +
                $"--retries=-1 --retries-sleep=60s --low-level-retries=10";
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.EnableRaisingEvents = true;

            var errorBuffer = new System.Text.StringBuilder();

            process.OutputDataReceived += (_, e) =>
            {
                if (string.IsNullOrEmpty(e.Data)) return;
                var match = TransferRegex.Match(e.Data);
                if (match.Success && progressTask != null)
                {
                    if (int.TryParse(match.Groups[1].Value, out var percentage))
                    {
                        progressTask.Value = percentage;
                        progressTask.Description = $"[cyan]Upload: {percentage}%[/]";
                    }
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

            var ok = process.ExitCode == 0;
            if (!ok && errorBuffer.Length > 0)
                AnsiConsole.MarkupLine($"[red]Erro no rclone upload: {errorBuffer.ToString().EscapeMarkup()}[/]");
            
            return ok;
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

    public static async Task<bool> DownloadFileAsync(string remoteRelativePath, string localFilePath)
    {
        var remote = RemoteBase + "/" + remoteRelativePath.TrimStart('/');
        return await CopyToAsync(remote, localFilePath);
    }


    public static async Task<bool> CopyToAsync(string remotePath, string localFilePath)
    {
        try
        {
            using var process = new Process();
            process.StartInfo.FileName = "rclone";
            process.StartInfo.Arguments = $"copyto \"{remotePath}\" \"{localFilePath}\" " +
                $"--checksum --retries=-1 --retries-sleep=60s --low-level-retries=10 --bwlimit=off";
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.EnableRaisingEvents = true;

            var errorBuffer = new System.Text.StringBuilder();

            process.ErrorDataReceived += (_, e) =>
            {
                if (string.IsNullOrEmpty(e.Data)) return;
                errorBuffer.AppendLine(e.Data);
            };

            process.Start();
            process.BeginErrorReadLine();
            process.BeginOutputReadLine();

            await process.WaitForExitAsync();

            var ok = process.ExitCode == 0 && File.Exists(localFilePath);
            if (!ok && errorBuffer.Length > 0)
            {
                AnsiConsole.MarkupLine($"[yellow]⚠️ rclone copyto falhou: {errorBuffer.ToString().EscapeMarkup()}[/]");
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

            using var process = new Process();
            process.StartInfo.FileName = "rclone";
            process.StartInfo.Arguments = $"copyto \"{localFilePath}\" \"{remotePath}\" " +
                $"--checksum --retries=-1 --retries-sleep=60s --low-level-retries=10 --bwlimit=off --no-update-modtime";
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.EnableRaisingEvents = true;

            var errorBuffer = new System.Text.StringBuilder();

            process.ErrorDataReceived += (_, e) =>
            {
                if (string.IsNullOrEmpty(e.Data)) return;
                errorBuffer.AppendLine(e.Data);
            };

            process.Start();
            process.BeginErrorReadLine();
            process.BeginOutputReadLine();

            await process.WaitForExitAsync();

            var ok = process.ExitCode == 0;
            if (!ok && errorBuffer.Length > 0)
            {
                AnsiConsole.MarkupLine($"[yellow]⚠️ rclone upload file falhou: {errorBuffer.ToString().EscapeMarkup()}[/]");
            }
            return ok;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[yellow]⚠️ Erro no rclone upload file: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
    }
}
