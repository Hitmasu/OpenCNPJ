using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Xml.Linq;
using CNPJExporter.Configuration;
using CNPJExporter.Utils;
using Spectre.Console;

namespace CNPJExporter.Downloaders;

public sealed record DavEntry(
    string Name,
    Uri Uri,
    bool IsCollection,
    long? ContentLength,
    string? ContentType,
    string? ETag,
    DateTimeOffset? LastModified);

public class WebDownloader
{
    private static readonly HttpMethod PropfindMethod = new("PROPFIND");

    private readonly string _downloadRootDir;
    private readonly string _extractRootDir;
    private readonly Uri _shareRoot;
    private readonly HttpClient _http;

    public WebDownloader(string downloadDir, string extractDir)
    {
        _downloadRootDir = downloadDir;
        _extractRootDir = extractDir;
        Directory.CreateDirectory(_downloadRootDir);
        Directory.CreateDirectory(_extractRootDir);

        var configuredRoot = AppConfig.Current.Downloader.PublicShareRoot;
        if (string.IsNullOrWhiteSpace(configuredRoot))
            throw new InvalidOperationException("Downloader.PublicShareRoot não foi configurado.");

        _shareRoot = new(configuredRoot.EndsWith('/') ? configuredRoot : configuredRoot + "/");

        _http = new()
        {
            Timeout = Timeout.InfiniteTimeSpan
        };
        _http.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("OpenCNPJ", "1.0"));
    }

    public async Task<string> DownloadAndExtractAsync(string? requestedMonth, CancellationToken ct = default)
    {
        var monthEntries = await ListDirectoryAsync(_shareRoot, ct);
        var availableMonths = monthEntries
            .Where(x => x.IsCollection && DatasetPathResolver.IsDatasetKey(x.Name))
            .Select(x => x.Name)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();

        if (availableMonths.Count == 0)
            throw new InvalidOperationException("Nenhuma pasta mensal encontrada no compartilhamento público da Receita.");

        var selectedMonth = ResolveTargetMonth(requestedMonth, availableMonths);
        var monthUri = new Uri(_shareRoot, $"{selectedMonth}/");
        var zipEntries = await ListDirectoryAsync(monthUri, ct);
        var zipFiles = zipEntries
            .Where(x => !x.IsCollection && x.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
            .OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (zipFiles.Count == 0)
            throw new InvalidOperationException($"Nenhum arquivo ZIP encontrado para {selectedMonth}.");

        var localDownloadDir = Path.Combine(_downloadRootDir, selectedMonth);
        var localExtractDir = Path.Combine(_extractRootDir, selectedMonth);
        Directory.CreateDirectory(localDownloadDir);
        Directory.CreateDirectory(localExtractDir);

        AnsiConsole.MarkupLine($"[blue]Mês selecionado:[/] [white]{selectedMonth}[/]");
        AnsiConsole.MarkupLine($"[blue]Fonte WebDAV:[/] [white]{monthUri}[/]");

        var localZips = await DownloadAllAsync(zipFiles, localDownloadDir, ct);
        await ExtractAllAsync(localZips, localExtractDir, ct);

        return selectedMonth;
    }

    private static string ResolveTargetMonth(string? requestedMonth, IReadOnlyCollection<string> availableMonths)
    {
        if (!string.IsNullOrWhiteSpace(requestedMonth))
        {
            if (availableMonths.Contains(requestedMonth, StringComparer.Ordinal))
                return requestedMonth;

            throw new InvalidOperationException($"Mês {requestedMonth} não encontrado no compartilhamento público.");
        }

        return availableMonths
            .OrderBy(x => x, StringComparer.Ordinal)
            .Last();
    }

    private async Task<List<DavEntry>> ListDirectoryAsync(Uri directoryUri, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(PropfindMethod, directoryUri);
        request.Headers.Add("Depth", "1");

        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();

        var xmlContent = await response.Content.ReadAsStringAsync(ct);
        return ParsePropfindResponse(xmlContent, directoryUri);
    }

    private static List<DavEntry> ParsePropfindResponse(string xmlContent, Uri requestUri)
    {
        var document = XDocument.Parse(xmlContent);
        XNamespace dav = "DAV:";
        var entries = new List<DavEntry>();
        var normalizedRequestPath = NormalizePath(requestUri.AbsolutePath);

        foreach (var responseElement in document.Descendants(dav + "response"))
        {
            var hrefValue = responseElement.Element(dav + "href")?.Value;
            if (string.IsNullOrWhiteSpace(hrefValue))
                continue;

            var entryUri = new Uri(requestUri, hrefValue);
            var normalizedEntryPath = NormalizePath(entryUri.AbsolutePath);
            if (string.Equals(normalizedEntryPath, normalizedRequestPath, StringComparison.Ordinal))
                continue;

            var name = Path.GetFileName(normalizedEntryPath.TrimEnd('/'));
            if (string.IsNullOrWhiteSpace(name))
                continue;

            var prop = responseElement
                .Elements(dav + "propstat")
                .Select(x => x.Element(dav + "prop"))
                .FirstOrDefault(x => x is not null);

            if (prop is null)
                continue;

            var isCollection = prop.Element(dav + "resourcetype")?.Element(dav + "collection") is not null;
            var contentLength = TryParseInt64(prop.Element(dav + "getcontentlength")?.Value);
            var contentType = prop.Element(dav + "getcontenttype")?.Value;
            var eTag = prop.Element(dav + "getetag")?.Value?.Trim('"');
            var lastModified = TryParseDate(prop.Element(dav + "getlastmodified")?.Value);

            entries.Add(new(
                Name: Uri.UnescapeDataString(name),
                Uri: entryUri,
                IsCollection: isCollection,
                ContentLength: contentLength,
                ContentType: contentType,
                ETag: eTag,
                LastModified: lastModified));
        }

        return entries;

        static string NormalizePath(string value)
        {
            return value.EndsWith("/", StringComparison.Ordinal) ? value : value + "/";
        }

        static long? TryParseInt64(string? value)
        {
            return long.TryParse(value, out var parsed) ? parsed : null;
        }

        static DateTimeOffset? TryParseDate(string? value)
        {
            return DateTimeOffset.TryParse(value, out var parsed) ? parsed : null;
        }
    }

    private async Task<List<string>> DownloadAllAsync(List<DavEntry> entries, string targetDir, CancellationToken ct)
    {
        var results = new ConcurrentBag<string>();

        await AnsiConsole.Progress()
            .Columns(
                new TaskDescriptionColumn { Alignment = Justify.Left },
                new ProgressBarColumn(),
                new PercentageColumn(),
                new RemainingTimeColumn(),
                new SpinnerColumn())
            .StartAsync(async ctx =>
            {
                var tasks = new Dictionary<string, ProgressTask>(StringComparer.Ordinal);

                foreach (var entry in entries)
                {
                    var filePath = Path.Combine(targetDir, entry.Name);
                    if (CanReuseLocalFile(filePath, entry))
                    {
                        var completed = ctx.AddTask($"[green]✓ {entry.Name} (já existe)[/]");
                        completed.Value = completed.MaxValue;
                        results.Add(filePath);
                        continue;
                    }

                    tasks[filePath] = ctx.AddTask($"[cyan]{entry.Name}[/]");
                }

                var toDownload = entries
                    .Select(entry => new
                    {
                        Entry = entry,
                        FilePath = Path.Combine(targetDir, entry.Name)
                    })
                    .Where(x => !CanReuseLocalFile(x.FilePath, x.Entry))
                    .ToList();

                await Parallel.ForEachAsync(
                    toDownload,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = Math.Max(1, AppConfig.Current.Downloader.ParallelDownloads),
                        CancellationToken = ct
                    },
                    async (item, token) =>
                    {
                        var downloadedPath = await DownloadOneAsync(item.Entry, item.FilePath, tasks[item.FilePath], token);
                        results.Add(downloadedPath);
                    });
            });

        return results.OrderBy(x => x, StringComparer.Ordinal).ToList();
    }

    private static bool CanReuseLocalFile(string filePath, DavEntry entry)
    {
        if (!File.Exists(filePath))
            return false;

        if (entry.ContentLength is null)
            return true;

        return new FileInfo(filePath).Length == entry.ContentLength.Value;
    }

    private async Task<string> DownloadOneAsync(DavEntry entry, string filePath, ProgressTask task, CancellationToken ct)
    {
        const int maxRetries = 3;

        if (File.Exists(filePath))
            File.Delete(filePath);

        for (var retry = 1; retry <= maxRetries; retry++)
        {
            try
            {
                using var response = await _http.GetAsync(entry.Uri, HttpCompletionOption.ResponseHeadersRead, ct);
                response.EnsureSuccessStatusCode();

                var total = response.Content.Headers.ContentLength ?? entry.ContentLength ?? 0;
                task.MaxValue = total > 0 ? total : 1_000_000;

                await using var src = await response.Content.ReadAsStreamAsync(ct);
                await using var dst = new FileStream(
                    filePath,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.Read,
                    1 << 20,
                    useAsync: true);

                var buffer = new byte[1 << 16];
                long readTotal = 0;
                int read;
                while ((read = await src.ReadAsync(buffer.AsMemory(0, buffer.Length), ct)) > 0)
                {
                    await dst.WriteAsync(buffer.AsMemory(0, read), ct);
                    readTotal += read;
                    if (total > 0)
                        task.Value = readTotal;
                    else
                        task.Increment(read);
                }

                task.Description = $"[green]✓ {entry.Name}[/]";
                task.Value = task.MaxValue;
                return filePath;
            }
            catch when (retry < maxRetries)
            {
                task.Description = $"[red]✗ {entry.Name} (tentativa {retry})[/]";
                await Task.Delay(TimeSpan.FromSeconds(retry), ct);
            }
        }

        throw new InvalidOperationException($"Falha ao baixar {entry.Name} após {maxRetries} tentativas.");
    }

    private static async Task ExtractAllAsync(List<string> zipFiles, string targetDir, CancellationToken ct)
    {
        Directory.CreateDirectory(targetDir);
        var markerPath = Path.Combine(targetDir, ".extract_complete");

        if (File.Exists(markerPath))
        {
            AnsiConsole.MarkupLine($"[blue]ℹ️ Extração já concluída anteriormente em {Path.GetFullPath(targetDir)}[/]");
            return;
        }

        var allSucceeded = true;

        await AnsiConsole.Progress().StartAsync(async ctx =>
        {
            foreach (var zipFile in zipFiles)
            {
                ct.ThrowIfCancellationRequested();
                var task = ctx.AddTask($"[yellow]Extraindo {Path.GetFileName(zipFile)}[/]", maxValue: 1);
                try
                {
                    System.IO.Compression.ZipFile.ExtractToDirectory(zipFile, targetDir, overwriteFiles: true);
                    task.Value = 1;
                }
                catch (Exception ex)
                {
                    allSucceeded = false;
                    task.Description = $"[red]Erro em {Path.GetFileName(zipFile)}: {ex.Message.EscapeMarkup()}[/]";
                }
            }

            await Task.CompletedTask;
        });

        if (!allSucceeded)
            throw new InvalidOperationException("Falha ao extrair um ou mais arquivos ZIP.");

        await File.WriteAllTextAsync(markerPath, DateTimeOffset.UtcNow.ToString("O"), ct);
        AnsiConsole.MarkupLine($"[green]✓ Extração concluída em {Path.GetFullPath(targetDir)}[/]");
    }
}
