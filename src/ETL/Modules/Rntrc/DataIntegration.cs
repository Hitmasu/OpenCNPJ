using CNPJExporter.Integrations;
using CNPJExporter.Modules.Rntrc.Configuration;
using CNPJExporter.Modules.Rntrc.Downloaders;
using CNPJExporter.Modules.Rntrc.Processors;

namespace CNPJExporter.Modules.Rntrc;

public sealed class DataIntegration : IDataIntegration, IDataIntegrationSourceProvider
{
    private readonly IntegrationOptions _options;
    private readonly Func<CancellationToken, Task<SourceFile>> _getSourceAsync;
    private readonly IRntrcSourceFileDownloader _sourceFileDownloader;
    private readonly ParquetProcessor _processor;

    public DataIntegration(IntegrationOptions options)
    {
        _options = options;
        var downloader = new Downloader(options);
        _getSourceAsync = downloader.GetSourceAsync;
        _sourceFileDownloader = new RntrcSourceFileDownloader(downloader);
        _processor = new ParquetProcessor();
        Descriptor = new DataIntegrationDescriptor(
            Key: "rntrc",
            JsonPropertyName: "rntrc",
            RefreshInterval: TimeSpan.FromHours(Math.Max(1, options.RefreshHours)),
            SchemaVersion: "1");
    }

    internal DataIntegration(
        IntegrationOptions options,
        Func<CancellationToken, Task<SourceFile>> getSourceAsync,
        IRntrcSourceFileDownloader sourceFileDownloader,
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

        var parquetPath = Path.Combine(context.ModuleParquetDir, "rntrc.parquet");

        var source = context.Source
                     ?? throw new InvalidOperationException(
                         "RNTRC requer Source no DataIntegrationRunContext. A pipeline raiz deve resolver a fonte e passá-la no RunAsync.");
        var csvPath = await _sourceFileDownloader.DownloadIfNeededAsync(source, context.ModuleWorkDir, cancellationToken);
        var moduleUpdatedAt = source.LastModified ?? context.Now;

        await _processor.ConvertToParquetAsync(
            csvPath,
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
}

internal interface IRntrcSourceFileDownloader
{
    Task<string> DownloadIfNeededAsync(
        SourceFile source,
        string destinationDirectory,
        CancellationToken cancellationToken = default);
}

internal sealed class RntrcSourceFileDownloader(Downloader downloader) : IRntrcSourceFileDownloader
{
    public Task<string> DownloadIfNeededAsync(
        SourceFile source,
        string destinationDirectory,
        CancellationToken cancellationToken = default) =>
        downloader.DownloadIfNeededAsync(source, destinationDirectory, cancellationToken);
}
