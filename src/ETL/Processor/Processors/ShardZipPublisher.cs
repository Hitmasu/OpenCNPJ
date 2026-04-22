using System.IO.Compression;
using System.Security.Cryptography;
using System.Diagnostics;
using CNPJExporter.Configuration;
using CNPJExporter.Exporters;
using CNPJExporter.Processors.Models;
using CNPJExporter.Utils;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class ShardZipPublisher : IShardZipPublisher
{
    private const string ZipFileName = "data.zip";
    private const string PublicZipBaseUrl = "https://file.opencnpj.org";

    public async Task<ZipArtifactPublication> PublishBaseAsync(
        string datasetKey,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        var normalizedReleaseId = releaseId.Trim('/');
        var releaseOutputDir = Path.Combine(
            DatasetPathResolver.GetDatasetPath(outputRootDir, datasetKey),
            "releases",
            normalizedReleaseId);
        var shardDir = Path.Combine(releaseOutputDir, AppConfig.Current.Shards.RemoteDir);

        return await PublishAsync(
            shardDir,
            Path.Combine(releaseOutputDir, ZipFileName),
            "receita",
            cancellationToken);
    }

    public async Task<ZipArtifactPublication> PublishModuleAsync(
        string moduleKey,
        string releaseId,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        var normalizedModuleKey = moduleKey.Trim('/');
        var normalizedReleaseId = releaseId.Trim('/');
        var localShardDir = Path.Combine(
            outputRootDir,
            "shards",
            "modules",
            normalizedModuleKey,
            "releases",
            normalizedReleaseId);

        return await PublishAsync(
            localShardDir,
            Path.Combine(localShardDir, ZipFileName),
            normalizedModuleKey,
            cancellationToken);
    }

    internal static async Task<ZipArtifactPublication> BuildLocalZipForTest(
        string sourceDir,
        string localZipPath,
        string datasetKey,
        CancellationToken cancellationToken = default)
    {
        var metadata = await BuildZipAsync(sourceDir, localZipPath, cancellationToken);
        return new ZipArtifactPublication(
            true,
            metadata.Size,
            BuildPublicZipUrl(datasetKey),
            metadata.Md5Checksum);
    }

    private static async Task<ZipArtifactPublication> PublishAsync(
        string preferredLocalShardDir,
        string localZipPath,
        string datasetKey,
        CancellationToken cancellationToken)
    {
        var shardSourceDir = preferredLocalShardDir;
        if (!HasShardFiles(shardSourceDir))
        {
            throw new InvalidOperationException(
                $"Não há shards locais em {shardSourceDir} para gerar o ZIP do dataset {datasetKey}.");
        }

        AnsiConsole.MarkupLine(
            $"[grey]ZIP do dataset {datasetKey.EscapeMarkup()}:[/] usando shards locais em [grey]{shardSourceDir.EscapeMarkup()}[/]");

        Directory.CreateDirectory(Path.GetDirectoryName(localZipPath)!);
        var shardFileCount = CountShardFiles(shardSourceDir);
        AnsiConsole.MarkupLine(
            $"[cyan]Compactando ZIP do dataset {datasetKey.EscapeMarkup()}...[/] [grey](arquivos: {shardFileCount}, destino: {localZipPath.EscapeMarkup()})[/]");
        var metadata = await BuildZipAsync(shardSourceDir, localZipPath, cancellationToken);
        var remoteZipPath = BuildRemoteZipPath(datasetKey);
        AnsiConsole.MarkupLine(
            $"[grey]ZIP do dataset {datasetKey.EscapeMarkup()} pronto:[/] [cyan]{FormatSize(metadata.Size)}[/], md5 [cyan]{metadata.Md5Checksum.EscapeMarkup()}[/]. [grey]Enviando para {remoteZipPath.EscapeMarkup()}[/]");
        var uploaded = await RcloneClient.UploadFileAsync(localZipPath, remoteZipPath);
        if (!uploaded)
            throw new InvalidOperationException($"Falha ao enviar ZIP publicado para {remoteZipPath}.");

        AnsiConsole.MarkupLine(
            $"[green]✓ ZIP do dataset {datasetKey.EscapeMarkup()} publicado[/] [grey](url: {BuildPublicZipUrl(datasetKey).EscapeMarkup()})[/]");
        return new ZipArtifactPublication(
            true,
            metadata.Size,
            BuildPublicZipUrl(datasetKey),
            metadata.Md5Checksum);
    }

    private static bool HasShardFiles(string shardDir)
    {
        if (!Directory.Exists(shardDir))
            return false;

        return Directory.EnumerateFiles(shardDir, "*", SearchOption.TopDirectoryOnly)
            .Any(path => path.EndsWith(".ndjson", StringComparison.OrdinalIgnoreCase));
    }

    private static async Task<(long Size, string Md5Checksum)> BuildZipAsync(
        string sourceDir,
        string localZipPath,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var files = Directory.Exists(sourceDir)
            ? Directory.EnumerateFiles(sourceDir, "*", SearchOption.TopDirectoryOnly)
                .Where(path => path.EndsWith(".ndjson", StringComparison.OrdinalIgnoreCase))
                .OrderBy(path => Path.GetFileName(path), StringComparer.Ordinal)
                .ToArray()
            : [];

        DeleteIfExists(localZipPath);
        await using (var stream = File.Create(localZipPath))
        await using (var archive = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: false))
        {
            foreach (var file in files)
            {
                var entry = archive.CreateEntry(Path.GetFileName(file), CompressionLevel.Optimal);
                await using var entryStream = entry.Open();
                await using var input = File.OpenRead(file);
                await input.CopyToAsync(entryStream, cancellationToken);
            }
        }

        await using var zipStream = File.OpenRead(localZipPath);
        var hash = await MD5.HashDataAsync(zipStream, cancellationToken);
        AnsiConsole.MarkupLine(
            $"[grey]Compactação concluída:[/] [cyan]{files.Length}[/] arquivos em [grey]{stopwatch.Elapsed:hh\\:mm\\:ss}[/]");
        return (new FileInfo(localZipPath).Length, Convert.ToHexString(hash).ToLowerInvariant());
    }

    private static int CountShardFiles(string sourceDir)
    {
        if (!Directory.Exists(sourceDir))
            return 0;

        return Directory.EnumerateFiles(sourceDir, "*", SearchOption.TopDirectoryOnly)
            .Count(path => path.EndsWith(".ndjson", StringComparison.OrdinalIgnoreCase));
    }

    private static string FormatSize(long bytes)
    {
        string[] units = ["B", "KB", "MB", "GB", "TB"];
        double value = bytes;
        var unitIndex = 0;
        while (value >= 1024 && unitIndex < units.Length - 1)
        {
            value /= 1024;
            unitIndex++;
        }

        return $"{value:0.##} {units[unitIndex]}";
    }

    private static string BuildRemoteZipPath(string datasetKey) =>
        $"releases/{datasetKey.Trim('/')}/{ZipFileName}";

    private static string BuildPublicZipUrl(string datasetKey) =>
        $"{PublicZipBaseUrl}/{BuildRemoteZipPath(datasetKey)}";

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
