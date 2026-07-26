using System.Globalization;
using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using CNPJExporter.Integrations;
using CNPJExporter.Modules.PortalTransparencia.Configuration;
using CNPJExporter.Modules.PortalTransparencia.Models;
using Spectre.Console;

namespace CNPJExporter.Modules.PortalTransparencia.Downloaders;

internal sealed record PortalArtifact(
    string DateToken,
    string ArchiveToken,
    Uri DownloadUri,
    string FileName,
    int Year,
    int? Month);

internal sealed record PortalArtifactSegment(
    string Id,
    IReadOnlyList<PortalArtifact> Artifacts,
    IReadOnlyList<string> ReplacesSegmentIds);

public sealed class Downloader
{
    private static readonly Regex ArtifactPattern = new(
        """
        arquivos\.push\(\s*\{\s*
        "ano"\s*:\s*"(?<year>\d{4})"\s*,\s*
        "mes"\s*:\s*"(?<month>\d{0,2})"\s*,\s*
        "dia"\s*:\s*"(?<day>\d{0,2})"\s*,\s*
        "origem"\s*:\s*"(?<source>[^"]+)"\s*
        \}\s*\)
        """,
        RegexOptions.Compiled
        | RegexOptions.CultureInvariant
        | RegexOptions.IgnorePatternWhitespace);
    private static readonly Regex EscapedUnicodeFileNamePattern = new(
        @"#U(?<code>[0-9A-F]{4})",
        RegexOptions.Compiled
        | RegexOptions.CultureInvariant
        | RegexOptions.IgnoreCase);
    private static readonly Encoding ZipEntryNameEncoding =
        CreateZipEntryNameEncoding();

    private readonly IntegrationOptions _options;
    private readonly PortalDatasetDefinition _definition;
    private string? _catalogHtml;
    private Uri? _catalogBaseUri;

    internal Downloader(
        IntegrationOptions options,
        PortalDatasetDefinition definition)
    {
        _options = options;
        _definition = definition;
    }

    public async Task<SourceFile> GetSourceAsync(CancellationToken cancellationToken = default)
    {
        var catalogBaseUri = GetCatalogBaseUri();
        var catalogUri = new Uri(
            $"{catalogBaseUri.AbsoluteUri.TrimEnd('/')}/{_definition.CatalogSlug}");

        using var http = CreateHttpClient();
        var catalogHtml = await http.GetStringAsync(catalogUri, cancellationToken);
        _catalogHtml = catalogHtml;
        _catalogBaseUri = catalogBaseUri;
        var artifact = SelectLatestArtifact(catalogHtml, catalogBaseUri, _definition);
        using var response = await SendMetadataRequestAsync(http, artifact.DownloadUri, cancellationToken);

        var contentLength = response.Content.Headers.ContentLength;
        var lastModified = response.Content.Headers.LastModified;
        var etag = response.Headers.ETag?.Tag;
        var catalogFingerprint = BuildCatalogFingerprint(
            SelectArtifacts(catalogHtml, catalogBaseUri, _definition));
        var sourceVersion = BuildSourceVersion(
            _definition.Key,
            $"{artifact.DateToken}:{catalogFingerprint}",
            etag,
            contentLength,
            lastModified);

        return new SourceFile(
            artifact.DownloadUri,
            artifact.FileName,
            sourceVersion,
            contentLength,
            lastModified,
            $"{_definition.Key} {artifact.DateToken}");
    }

    internal async Task<IReadOnlyList<PortalArtifactSegment>> GetHistoricalSegmentsAsync(
        int currentYear,
        CancellationToken cancellationToken = default)
    {
        if (!_definition.IsSegmented)
        {
            throw new InvalidOperationException(
                $"O dataset {_definition.Key} não é histórico segmentado.");
        }

        var catalogBaseUri = _catalogBaseUri ?? GetCatalogBaseUri();
        var catalogHtml = _catalogHtml;
        if (catalogHtml is null)
        {
            var catalogUri = new Uri(
                $"{catalogBaseUri.AbsoluteUri.TrimEnd('/')}/{_definition.CatalogSlug}");
            using var http = CreateHttpClient();
            catalogHtml = await http.GetStringAsync(catalogUri, cancellationToken);
            _catalogHtml = catalogHtml;
            _catalogBaseUri = catalogBaseUri;
        }

        var artifacts = SelectArtifacts(
            catalogHtml,
            catalogBaseUri,
            _definition);
        var unavailable = artifacts
            .Where(artifact =>
                _definition.EffectiveKnownUnavailableDateTokens.Contains(
                    artifact.DateToken))
            .Select(artifact => artifact.DateToken)
            .ToArray();
        if (unavailable.Length > 0)
        {
            AnsiConsole.MarkupLine(
                $"[yellow]⚠ {_definition.Key.EscapeMarkup()}: arquivos oficiais indisponíveis/corrompidos serão registrados como lacuna: {string.Join(", ", unavailable).EscapeMarkup()}[/]");
        }

        return GroupHistoricalArtifacts(
            artifacts,
            _definition,
            currentYear);
    }

    internal async Task<ExtractedDataset> DownloadAndExtractAsync(
        SourceFile source,
        string destinationDirectory,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(destinationDirectory);
        var zipPath = Path.Combine(destinationDirectory, source.FileName);

        if (!CanReuseLocalFile(zipPath, source))
            await DownloadAsync(source, zipPath, cancellationToken);
        else
            AnsiConsole.MarkupLine(
                $"[green]✓ {_definition.Key.EscapeMarkup()} {source.FileName.EscapeMarkup()} já está baixado[/]");

        return await ExtractRequiredCsvsAsync(zipPath, source, cancellationToken);
    }

    internal Task<ExtractedDataset> DownloadAndExtractArtifactAsync(
        PortalArtifact artifact,
        SourceFile catalogSource,
        string destinationDirectory,
        CancellationToken cancellationToken = default)
    {
        var isCatalogHead = artifact.DownloadUri == catalogSource.Uri;
        var source = isCatalogHead
            ? catalogSource with
            {
                FileName = artifact.FileName,
                DisplayName = $"{_definition.Key} {artifact.DateToken}"
            }
            : new SourceFile(
                artifact.DownloadUri,
                artifact.FileName,
                BuildArtifactSourceVersion(_definition.Key, artifact),
                null,
                null,
                $"{_definition.Key} {artifact.DateToken}");

        return DownloadAndExtractAsync(source, destinationDirectory, cancellationToken);
    }

    internal static PortalArtifact SelectLatestArtifactForTest(
        string catalogHtml,
        Uri catalogBaseUri,
        string datasetKey) =>
        SelectLatestArtifact(
            catalogHtml,
            catalogBaseUri,
            PortalDatasetDefinition.GetRequired(datasetKey));

    internal static IReadOnlyList<PortalArtifact> SelectArtifactsForTest(
        string catalogHtml,
        Uri catalogBaseUri,
        string datasetKey) =>
        SelectArtifacts(
            catalogHtml,
            catalogBaseUri,
            PortalDatasetDefinition.GetRequired(datasetKey));

    internal static IReadOnlyList<PortalArtifactSegment> GroupHistoricalArtifactsForTest(
        IReadOnlyList<PortalArtifact> artifacts,
        string datasetKey,
        int currentYear) =>
        GroupHistoricalArtifacts(
            artifacts,
            PortalDatasetDefinition.GetRequired(datasetKey),
            currentYear);

    internal static bool CanReuseLocalFileForTest(string filePath, SourceFile source) =>
        CanReuseLocalFile(filePath, source);

    internal static Task WriteSourceMetadataForTestAsync(string filePath, SourceFile source) =>
        WriteSourceMetadataAsync(filePath, source, CancellationToken.None);

    private Uri GetCatalogBaseUri()
    {
        if (!Uri.TryCreate(_options.CatalogBaseUrl, UriKind.Absolute, out var uri)
            || (uri.Scheme != Uri.UriSchemeHttps && uri.Scheme != Uri.UriSchemeHttp))
        {
            throw new InvalidOperationException(
                "PortalTransparenciaIntegration.CatalogBaseUrl deve ser uma URL HTTP(S) absoluta.");
        }

        return uri;
    }

    private static PortalArtifact SelectLatestArtifact(
        string catalogHtml,
        Uri catalogBaseUri,
        PortalDatasetDefinition definition)
    {
        var artifacts = SelectArtifacts(catalogHtml, catalogBaseUri, definition);

        return artifacts.LastOrDefault()
               ?? throw new InvalidOperationException(
                   $"Nenhum arquivo disponível foi encontrado no catálogo de {definition.Key}.");
    }

    private static IReadOnlyList<PortalArtifact> SelectArtifacts(
        string catalogHtml,
        Uri catalogBaseUri,
        PortalDatasetDefinition definition)
    {
        if (definition.ArtifactPeriodicity == PortalArtifactPeriodicity.Singleton)
        {
            return
            [
                new PortalArtifact(
                    "UNICO",
                    definition.ArchiveToken,
                    new Uri(
                        $"{catalogBaseUri.AbsoluteUri.TrimEnd('/')}/{definition.CatalogSlug}/UNICO"),
                    $"{definition.ArchiveToken}.zip",
                    0,
                    null)
            ];
        }

        return ArtifactPattern
            .Matches(catalogHtml)
            .Select(match => new
            {
                Year = match.Groups["year"].Value,
                Month = match.Groups["month"].Value,
                Day = match.Groups["day"].Value,
                Source = match.Groups["source"].Value
            })
            .Where(candidate => string.Equals(
                candidate.Source,
                definition.ArchiveToken,
                StringComparison.OrdinalIgnoreCase))
            .Select(candidate =>
            {
                var dateToken = candidate.Year + candidate.Month + candidate.Day;
                var year = int.Parse(candidate.Year, CultureInfo.InvariantCulture);
                var month = string.IsNullOrEmpty(candidate.Month)
                    ? (int?)null
                    : int.Parse(candidate.Month, CultureInfo.InvariantCulture);
                var downloadUri = new Uri(
                    $"{catalogBaseUri.AbsoluteUri.TrimEnd('/')}/{definition.CatalogSlug}/{dateToken}");
                return new PortalArtifact(
                    dateToken,
                    candidate.Source,
                    downloadUri,
                    $"{dateToken}_{candidate.Source}.zip",
                    year,
                    month);
            })
            .OrderBy(artifact => artifact.DateToken, StringComparer.Ordinal)
            .ToArray();
    }

    private static IReadOnlyList<PortalArtifactSegment> GroupHistoricalArtifacts(
        IReadOnlyList<PortalArtifact> artifacts,
        PortalDatasetDefinition definition,
        int currentYear)
    {
        if (currentYear < PortalDatasetDefinition.HistoricalMinimumYear)
            throw new ArgumentOutOfRangeException(nameof(currentYear));

        var eligible = artifacts
            .Where(artifact => artifact.Year >= PortalDatasetDefinition.HistoricalMinimumYear)
            .Where(artifact => artifact.Year <= currentYear)
            .Where(artifact =>
                !definition.EffectiveKnownUnavailableDateTokens.Contains(
                    artifact.DateToken))
            .ToArray();

        var groups = eligible.GroupBy(artifact =>
            definition.ArtifactPeriodicity == PortalArtifactPeriodicity.Monthly
            && artifact.Year == currentYear
                ? $"{artifact.Year:D4}-{artifact.Month
                    ?? throw new InvalidDataException(
                        $"O artefato mensal {artifact.DateToken} não informa o mês."):D2}"
                : $"{artifact.Year:D4}");

        return groups
            .Select(group =>
            {
                var isClosedMonthlyYear =
                    definition.ArtifactPeriodicity == PortalArtifactPeriodicity.Monthly
                    && group.Key.Length == 4;
                var replacements = isClosedMonthlyYear
                    ? Enumerable.Range(1, 12)
                        .Select(month => $"{group.Key}-{month:D2}")
                        .ToArray()
                    : [];
                return new PortalArtifactSegment(
                    group.Key,
                    group.OrderBy(
                            artifact => artifact.DateToken,
                            StringComparer.Ordinal)
                        .ToArray(),
                    replacements);
            })
            .OrderBy(segment => segment.Id, StringComparer.Ordinal)
            .ToArray();
    }

    internal static string BuildSegmentSourceVersion(
        string datasetKey,
        PortalArtifactSegment segment,
        SourceFile catalogSource)
    {
        var latestArtifact = segment.Artifacts.Last();
        var includesCatalogHead = latestArtifact.DownloadUri == catalogSource.Uri;
        var seed = string.Join(
            '|',
            datasetKey,
            segment.Id,
            string.Join(',', segment.Artifacts.Select(artifact => artifact.DateToken)),
            includesCatalogHead ? catalogSource.SourceVersion : "closed");
        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(seed)))
            .ToLowerInvariant();
    }

    private static string BuildCatalogFingerprint(
        IReadOnlyList<PortalArtifact> artifacts)
    {
        var seed = string.Join(
            '|',
            artifacts.Select(artifact =>
                $"{artifact.DateToken}:{artifact.ArchiveToken}"));
        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(seed)))
            .ToLowerInvariant();
    }

    private static string BuildArtifactSourceVersion(
        string datasetKey,
        PortalArtifact artifact)
    {
        var seed = $"{datasetKey}|{artifact.DateToken}|{artifact.ArchiveToken}";
        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(seed)))
            .ToLowerInvariant();
    }

    private async Task DownloadAsync(
        SourceFile source,
        string zipPath,
        CancellationToken cancellationToken)
    {
        var partialPath = zipPath + ".part";
        DeleteIfExists(partialPath);

        try
        {
            AnsiConsole.MarkupLine(
                $"[cyan]Baixando {_definition.Key.EscapeMarkup()} {source.FileName.EscapeMarkup()}...[/]");
            using var http = CreateHttpClient();
            using var response = await http.GetAsync(
                source.Uri,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken);
            response.EnsureSuccessStatusCode();

            await using (var input = await response.Content.ReadAsStreamAsync(cancellationToken))
            await using (var output = new FileStream(
                             partialPath,
                             FileMode.Create,
                             FileAccess.Write,
                             FileShare.Read,
                             1 << 20,
                             FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                await input.CopyToAsync(output, cancellationToken);
            }

            if (source.ContentLength is not null
                && new FileInfo(partialPath).Length != source.ContentLength.Value)
            {
                throw new InvalidDataException(
                    $"Download incompleto de {_definition.Key}: esperado {source.ContentLength.Value} bytes.");
            }

            File.Move(partialPath, zipPath, overwrite: true);
            await WriteSourceMetadataAsync(zipPath, source, cancellationToken);
        }
        finally
        {
            DeleteIfExists(partialPath);
        }
    }

    private async Task<ExtractedDataset> ExtractRequiredCsvsAsync(
        string zipPath,
        SourceFile source,
        CancellationToken cancellationToken)
    {
        var extractionDirectory = Path.Combine(
            Path.GetDirectoryName(zipPath)!,
            "extracted",
            source.SourceVersion);
        var completionMarker = Path.Combine(extractionDirectory, ".complete");

        if (File.Exists(completionMarker))
        {
            var cached = TryResolveExtractedDataset(extractionDirectory);
            if (cached is not null)
                return cached;
        }

        if (Directory.Exists(extractionDirectory))
            Directory.Delete(extractionDirectory, recursive: true);
        Directory.CreateDirectory(extractionDirectory);

        try
        {
            using var archive = ZipFile.Open(
                zipPath,
                ZipArchiveMode.Read,
                ZipEntryNameEncoding);
            var outputPaths = new List<string>(_definition.RequiredCsvSuffixes.Count);

            foreach (var suffix in _definition.RequiredCsvSuffixes)
            {
                var matches = archive.Entries
                    .Where(entry => !string.IsNullOrWhiteSpace(entry.Name))
                    .Select(entry => new
                    {
                        Entry = entry,
                        NormalizedName = NormalizeArchiveEntryName(entry.Name)
                    })
                    .Where(entry => entry.NormalizedName.EndsWith(
                        suffix,
                        StringComparison.OrdinalIgnoreCase))
                    .ToArray();
                if (matches.Length != 1)
                {
                    throw new InvalidDataException(
                        $"O ZIP de {_definition.Key} deve conter exatamente um arquivo *{suffix}; encontrados: {matches.Length}.");
                }

                var entry = matches[0].Entry;
                var outputPath = Path.Combine(
                    extractionDirectory,
                    Path.GetFileName(matches[0].NormalizedName));
                await using var input = entry.Open();
                await using var output = new FileStream(
                    outputPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.Read,
                    1 << 20,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);
                await input.CopyToAsync(output, cancellationToken);
                outputPaths.Add(outputPath);
            }

            await File.WriteAllTextAsync(completionMarker, source.SourceVersion, cancellationToken);
            return new ExtractedDataset(outputPaths);
        }
        catch
        {
            if (Directory.Exists(extractionDirectory))
                Directory.Delete(extractionDirectory, recursive: true);
            throw;
        }
    }

    private static string NormalizeArchiveEntryName(string fileName) =>
        EscapedUnicodeFileNamePattern.Replace(
            fileName,
            match => ((char)int.Parse(
                match.Groups["code"].Value,
                NumberStyles.HexNumber,
                CultureInfo.InvariantCulture)).ToString());

    private static Encoding CreateZipEntryNameEncoding()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        return Encoding.GetEncoding(437);
    }

    private ExtractedDataset? TryResolveExtractedDataset(string extractionDirectory)
    {
        var csvPaths = Directory
            .EnumerateFiles(extractionDirectory, "*.csv", SearchOption.TopDirectoryOnly)
            .ToArray();
        var extracted = new ExtractedDataset(csvPaths);

        try
        {
            foreach (var suffix in _definition.RequiredCsvSuffixes)
                _ = extracted.RequireFileEnding(suffix);
            return extracted;
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    private static async Task<HttpResponseMessage> SendMetadataRequestAsync(
        HttpClient http,
        Uri uri,
        CancellationToken cancellationToken)
    {
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, uri);
        var response = await http.SendAsync(
            headRequest,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);

        if (response.StatusCode != HttpStatusCode.MethodNotAllowed
            && response.StatusCode != HttpStatusCode.NotImplemented)
        {
            response.EnsureSuccessStatusCode();
            return response;
        }

        response.Dispose();
        using var getRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        response = await http.SendAsync(
            getRequest,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);
        response.EnsureSuccessStatusCode();
        return response;
    }

    private static string BuildSourceVersion(
        string datasetKey,
        string dateToken,
        string? etag,
        long? contentLength,
        DateTimeOffset? lastModified)
    {
        var seed = string.Join(
            '|',
            datasetKey,
            dateToken,
            etag ?? "unknown",
            contentLength?.ToString(CultureInfo.InvariantCulture) ?? "unknown",
            lastModified?.ToString("O", CultureInfo.InvariantCulture) ?? "unknown");
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

        var metadata = ReadSourceMetadata(filePath);
        return metadata is not null
               && string.Equals(metadata.SourceVersion, source.SourceVersion, StringComparison.Ordinal)
               && (source.LastModified is null || metadata.LastModified == source.LastModified);
    }

    private static SourceFileMetadata? ReadSourceMetadata(string filePath)
    {
        var metadataPath = GetSourceMetadataPath(filePath);
        if (!File.Exists(metadataPath))
            return null;

        try
        {
            return JsonSerializer.Deserialize(
                File.ReadAllText(metadataPath),
                PortalTransparenciaJsonContext.Default.SourceFileMetadata);
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
        var metadataPath = GetSourceMetadataPath(filePath);
        var partialPath = metadataPath + ".part";
        var metadata = new SourceFileMetadata(
            source.SourceVersion,
            source.ContentLength,
            source.LastModified);
        var json = JsonSerializer.Serialize(
            metadata,
            PortalTransparenciaJsonContext.Default.SourceFileMetadata);

        try
        {
            await File.WriteAllTextAsync(partialPath, json, cancellationToken);
            File.Move(partialPath, metadataPath, overwrite: true);
        }
        finally
        {
            DeleteIfExists(partialPath);
        }
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

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

}

internal sealed record SourceFileMetadata(
    string SourceVersion,
    long? ContentLength,
    DateTimeOffset? LastModified);

[JsonSerializable(typeof(SourceFileMetadata))]
internal partial class PortalTransparenciaJsonContext : JsonSerializerContext;
