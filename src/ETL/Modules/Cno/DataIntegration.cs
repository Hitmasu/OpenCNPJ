using CNPJExporter.Integrations;
using CNPJExporter.Modules.Cno.Configuration;
using CNPJExporter.Modules.Cno.Downloaders;
using CNPJExporter.Modules.Cno.Processors;

namespace CNPJExporter.Modules.Cno;

public sealed class DataIntegration : IDataIntegration, IDataIntegrationSourceProvider
{
    private readonly IntegrationOptions _options;
    private readonly Func<CancellationToken, Task<SourceFile>> _getSourceAsync;
    private readonly ICnoSourceFileDownloader _sourceFileDownloader;
    private readonly ParquetProcessor _processor;

    public DataIntegration(IntegrationOptions options)
    {
        _options = options;
        var downloader = new Downloader(options);
        _getSourceAsync = downloader.GetSourceAsync;
        _sourceFileDownloader = new CnoSourceFileDownloader(downloader);
        _processor = new ParquetProcessor();
        Descriptor = new DataIntegrationDescriptor(
            Key: "cno",
            JsonPropertyName: "cno",
            RefreshInterval: TimeSpan.FromHours(Math.Max(1, options.RefreshHours)),
            SchemaVersion: "2");
    }

    internal DataIntegration(
        IntegrationOptions options,
        Func<CancellationToken, Task<SourceFile>> getSourceAsync,
        ICnoSourceFileDownloader sourceFileDownloader,
        ParquetProcessor? processor = null)
        : this(options)
    {
        _getSourceAsync = getSourceAsync;
        _sourceFileDownloader = sourceFileDownloader;
        _processor = processor ?? new ParquetProcessor();
    }

    public DataIntegrationDescriptor Descriptor { get; }

    public Task<SourceFile> GetSourceAsync(CancellationToken cancellationToken = default) =>
        _getSourceAsync(cancellationToken);

    public async Task<DataIntegrationRunResult> RunAsync(
        DataIntegrationRunContext context,
        CancellationToken cancellationToken = default)
    {
        Descriptor.Validate();
        Directory.CreateDirectory(context.ModuleWorkDir);
        Directory.CreateDirectory(context.ModuleParquetDir);

        var parquetPath = Path.Combine(context.ModuleParquetDir, "cno.parquet");

        var source = context.Source
                     ?? throw new InvalidOperationException(
                         "CNO requer Source no DataIntegrationRunContext. A pipeline raiz deve resolver a fonte e passá-la no RunAsync.");
        var zipPath = await _sourceFileDownloader.DownloadIfNeededAsync(source, context.ModuleWorkDir, cancellationToken);
        var extractDir = Path.Combine(context.ModuleWorkDir, "extracted", ToPathSegment(source.SourceVersion));
        var extractedFiles = await _processor.ExtractAsync(zipPath, extractDir, cancellationToken);
        var moduleUpdatedAt = source.LastModified ?? context.Now;

        await _processor.ConvertToParquetAsync(
            extractedFiles,
            parquetPath,
            moduleUpdatedAt,
            Math.Max(1, _options.ShardPrefixLength),
            cancellationToken);

        var hashes = await _processor.LoadHashesAsync(parquetPath, cancellationToken);
        return new DataIntegrationRunResult(
            source.SourceVersion,
            moduleUpdatedAt,
            parquetPath,
            hashes.Count,
            hashes);
    }

    private static string ToPathSegment(string value)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var chars = value.Select(ch => invalid.Contains(ch) ? '_' : ch).ToArray();
        var segment = new string(chars).Trim();
        return string.IsNullOrWhiteSpace(segment) ? "unknown" : segment;
    }
}

internal interface ICnoSourceFileDownloader
{
    Task<string> DownloadIfNeededAsync(
        SourceFile source,
        string destinationDirectory,
        CancellationToken cancellationToken = default);
}

internal sealed class CnoSourceFileDownloader(Downloader downloader) : ICnoSourceFileDownloader
{
    public Task<string> DownloadIfNeededAsync(
        SourceFile source,
        string destinationDirectory,
        CancellationToken cancellationToken = default) =>
        downloader.DownloadIfNeededAsync(source, destinationDirectory, cancellationToken);
}
