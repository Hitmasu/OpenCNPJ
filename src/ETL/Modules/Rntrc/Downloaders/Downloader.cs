using System.Globalization;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CNPJExporter.Modules.Rntrc.Configuration;
using CNPJExporter.Modules.Rntrc.Models;
using Spectre.Console;

namespace CNPJExporter.Modules.Rntrc.Downloaders;

public sealed class Downloader
{
    private readonly IntegrationOptions _options;

    public Downloader(IntegrationOptions options)
    {
        _options = options;
    }

    public async Task<SourceFile> GetSourceFileAsync(CancellationToken cancellationToken = default)
    {
        using var http = CreateHttpClient();
        var packageJson = await http.GetStringAsync(_options.PackageShowUrl, cancellationToken);
        return SelectLatestCsvResource(packageJson);
    }

    public async Task<string> DownloadIfNeededAsync(
        SourceFile source,
        string destinationDirectory,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(destinationDirectory);
        var csvPath = Path.Combine(destinationDirectory, source.FileName);

        if (CanReuseLocalFile(csvPath, source))
        {
            AnsiConsole.MarkupLine($"[green]✓ RNTRC {source.FileName.EscapeMarkup()} já está baixado[/]");
            return csvPath;
        }

        AnsiConsole.MarkupLine($"[cyan]Baixando RNTRC {source.FileName.EscapeMarkup()}...[/]");
        using var http = CreateHttpClient();
        using var response = await http.GetAsync(source.Uri, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        await using (var input = await response.Content.ReadAsStreamAsync(cancellationToken))
        await using (var output = new FileStream(csvPath, FileMode.Create, FileAccess.Write, FileShare.Read, 1 << 20, useAsync: true))
        {
            await input.CopyToAsync(output, cancellationToken);
        }

        await WriteSourceMetadataAsync(csvPath, source, cancellationToken);
        return csvPath;
    }

    internal static SourceFile SelectLatestCsvResourceForTest(string packageJson) =>
        SelectLatestCsvResource(packageJson);

    internal static bool CanReuseLocalFileForTest(string filePath, SourceFile source) =>
        CanReuseLocalFile(filePath, source);

    internal static Task WriteSourceMetadataForTestAsync(string filePath, SourceFile source) =>
        WriteSourceMetadataAsync(filePath, source, CancellationToken.None);

    private static SourceFile SelectLatestCsvResource(string packageJson)
    {
        using var document = JsonDocument.Parse(packageJson);
        if (!document.RootElement.TryGetProperty("success", out var successElement)
            || successElement.ValueKind != JsonValueKind.True)
        {
            throw new InvalidOperationException("Resposta CKAN do RNTRC não indica sucesso.");
        }

        if (!document.RootElement.TryGetProperty("result", out var resultElement)
            || !resultElement.TryGetProperty("resources", out var resourcesElement)
            || resourcesElement.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("Resposta CKAN do RNTRC não contém resources.");
        }

        var candidates = resourcesElement
            .EnumerateArray()
            .Select(ReadResource)
            .Where(static resource => resource is not null)
            .Select(static resource => resource!)
            .OrderBy(static resource => resource.Position ?? int.MinValue)
            .ThenBy(static resource => resource.LastModified ?? DateTimeOffset.MinValue)
            .ToArray();

        var latest = candidates.LastOrDefault()
            ?? throw new InvalidOperationException("Nenhum recurso CSV encontrado no pacote CKAN do RNTRC.");

        var fileName = Path.GetFileName(latest.Uri.LocalPath);
        if (string.IsNullOrWhiteSpace(fileName))
            fileName = $"rntrc-{latest.LastModified?.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture) ?? "latest"}.csv";

        return new SourceFile(
            latest.Uri,
            latest.Name,
            fileName,
            BuildSourceVersion(latest),
            latest.Size,
            latest.LastModified);
    }

    private static ResourceCandidate? ReadResource(JsonElement resourceElement)
    {
        var format = GetString(resourceElement, "format");
        if (!string.Equals(format, "CSV", StringComparison.OrdinalIgnoreCase))
            return null;

        var url = GetString(resourceElement, "url");
        if (string.IsNullOrWhiteSpace(url) || !Uri.TryCreate(url, UriKind.Absolute, out var uri))
            return null;

        return new ResourceCandidate(
            GetString(resourceElement, "name") ?? Path.GetFileName(uri.LocalPath),
            uri,
            TryGetInt(resourceElement, "position"),
            TryGetLong(resourceElement, "size"),
            TryParseDate(GetString(resourceElement, "last_modified")));
    }

    private static string BuildSourceVersion(ResourceCandidate resource)
    {
        var lastModified = resource.LastModified?.ToString("O", CultureInfo.InvariantCulture) ?? "unknown";
        var size = resource.Size?.ToString(CultureInfo.InvariantCulture) ?? "unknown";
        var seed = $"{resource.Uri}|{lastModified}|{size}";
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(seed))).ToLowerInvariant();
    }

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

    private static string? GetString(JsonElement element, string propertyName)
    {
        return element.TryGetProperty(propertyName, out var property)
               && property.ValueKind == JsonValueKind.String
            ? property.GetString()
            : null;
    }

    private static int? TryGetInt(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
            return null;

        return property.ValueKind switch
        {
            JsonValueKind.Number when property.TryGetInt32(out var value) => value,
            JsonValueKind.String when int.TryParse(property.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) => value,
            _ => null
        };
    }

    private static long? TryGetLong(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
            return null;

        return property.ValueKind switch
        {
            JsonValueKind.Number when property.TryGetInt64(out var value) => value,
            JsonValueKind.String when long.TryParse(property.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) => value,
            _ => null
        };
    }

    private static DateTimeOffset? TryParseDate(string? value) =>
        DateTimeOffset.TryParse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var parsed)
            ? parsed
            : null;

    private sealed record ResourceCandidate(
        string Name,
        Uri Uri,
        int? Position,
        long? Size,
        DateTimeOffset? LastModified);

    private sealed record SourceFileMetadata(
        string SourceVersion,
        long? ContentLength,
        DateTimeOffset? LastModified);
}
