using System.Globalization;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;
using CNPJExporter.Modules.Cno.Configuration;
using CNPJExporter.Modules.Cno.Models;
using Spectre.Console;

namespace CNPJExporter.Modules.Cno.Downloaders;

public sealed class Downloader
{
    private readonly IntegrationOptions _options;

    public Downloader(IntegrationOptions options)
    {
        _options = options;
    }

    public async Task<SourceFile> GetSourceFileAsync(CancellationToken cancellationToken = default)
    {
        var sourceRoot = new Uri(_options.PublicShareRoot.EndsWith("/", StringComparison.Ordinal)
            ? _options.PublicShareRoot
            : _options.PublicShareRoot + "/");

        using var http = CreateHttpClient();
        using var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), sourceRoot);
        request.Headers.Add("Depth", "1");

        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        var xml = await response.Content.ReadAsStringAsync(cancellationToken);
        XNamespace dav = "DAV:";
        var document = XDocument.Parse(xml);

        foreach (var responseElement in document.Descendants(dav + "response"))
        {
            var href = responseElement.Element(dav + "href")?.Value;
            if (string.IsNullOrWhiteSpace(href))
                continue;

            var name = Path.GetFileName(Uri.UnescapeDataString(href.TrimEnd('/')));
            if (!string.Equals(name, _options.ZipFileName, StringComparison.OrdinalIgnoreCase))
                continue;

            var prop = responseElement
                .Elements(dav + "propstat")
                .Select(x => x.Element(dav + "prop"))
                .FirstOrDefault(static x => x is not null);
            if (prop is null)
                continue;

            var eTag = prop.Element(dav + "getetag")?.Value?.Trim('"');
            var length = TryParseLong(prop.Element(dav + "getcontentlength")?.Value);
            var lastModified = TryParseDate(prop.Element(dav + "getlastmodified")?.Value);
            var sourceVersionSeed = string.IsNullOrWhiteSpace(eTag)
                ? $"{sourceRoot}|{name}|{lastModified?.ToString("O", CultureInfo.InvariantCulture) ?? "unknown"}|{length?.ToString(CultureInfo.InvariantCulture) ?? "unknown"}"
                : eTag;
            return new SourceFile(
                new Uri(sourceRoot, name),
                name,
                NormalizeSourceVersion(sourceVersionSeed),
                length,
                lastModified);
        }

        throw new InvalidOperationException($"Arquivo {_options.ZipFileName} não encontrado na pasta CNO.");
    }

    public async Task<string> DownloadIfNeededAsync(
        SourceFile source,
        string destinationDirectory,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(destinationDirectory);
        var zipPath = Path.Combine(destinationDirectory, source.FileName);

        if (CanReuseLocalFile(zipPath, source))
        {
            AnsiConsole.MarkupLine($"[green]✓ CNO {source.FileName.EscapeMarkup()} já está baixado[/]");
            return zipPath;
        }

        AnsiConsole.MarkupLine($"[cyan]Baixando CNO {source.FileName.EscapeMarkup()}...[/]");
        using var http = CreateHttpClient();
        using var response = await http.GetAsync(source.Uri, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        await using (var input = await response.Content.ReadAsStreamAsync(cancellationToken))
        await using (var output = new FileStream(zipPath, FileMode.Create, FileAccess.Write, FileShare.Read, 1 << 20, useAsync: true))
        {
            await input.CopyToAsync(output, cancellationToken);
        }

        await WriteSourceMetadataAsync(zipPath, source, cancellationToken);
        return zipPath;
    }

    internal static bool CanReuseLocalFileForTest(string filePath, SourceFile source) =>
        CanReuseLocalFile(filePath, source);

    internal static Task WriteSourceMetadataForTestAsync(string filePath, SourceFile source) =>
        WriteSourceMetadataAsync(filePath, source, CancellationToken.None);

    private static bool CanReuseLocalFile(string filePath, SourceFile source)
    {
        if (!File.Exists(filePath))
            return false;

        if (source.ContentLength is not null
            && new FileInfo(filePath).Length != source.ContentLength.Value)
        {
            return false;
        }

        if (string.Equals(source.SourceVersion, "unknown", StringComparison.Ordinal))
            return false;

        var metadata = ReadSourceMetadata(filePath);
        if (metadata is null)
            return false;

        if (!string.Equals(metadata.SourceVersion, source.SourceVersion, StringComparison.Ordinal))
            return false;

        return source.LastModified is null || metadata.LastModified == source.LastModified;
    }

    private static SourceFileMetadata? ReadSourceMetadata(string filePath)
    {
        var metadataPath = GetSourceMetadataPath(filePath);
        if (!File.Exists(metadataPath))
            return null;

        try
        {
            var json = File.ReadAllText(metadataPath);
            return JsonSerializer.Deserialize<SourceFileMetadata>(json);
        }
        catch
        {
            return null;
        }
    }

    private static async Task WriteSourceMetadataAsync(
        string filePath,
        SourceFile source,
        CancellationToken cancellationToken)
    {
        var metadata = new SourceFileMetadata(
            source.SourceVersion,
            source.ContentLength,
            source.LastModified);
        var json = JsonSerializer.Serialize(metadata);
        await File.WriteAllTextAsync(GetSourceMetadataPath(filePath), json, cancellationToken);
    }

    private static string GetSourceMetadataPath(string filePath) => filePath + ".source.json";

    private static HttpClient CreateHttpClient()
    {
        var http = new HttpClient
        {
            Timeout = Timeout.InfiniteTimeSpan
        };
        http.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("OpenCNPJ", "1.0"));
        return http;
    }

    private static long? TryParseLong(string? value) =>
        long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;

    private static DateTimeOffset? TryParseDate(string? value) =>
        DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed) ? parsed : null;

    private static string NormalizeSourceVersion(string value)
    {
        var normalized = value.Trim().Trim('"').ToLowerInvariant();
        if ((normalized.Length is 32 or 40 or 64)
            && normalized.All(Uri.IsHexDigit))
        {
            return normalized;
        }

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    }

    private sealed record SourceFileMetadata(
        string SourceVersion,
        long? ContentLength,
        DateTimeOffset? LastModified);
}
