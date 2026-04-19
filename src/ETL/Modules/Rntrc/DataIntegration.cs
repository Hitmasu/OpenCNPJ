using CNPJExporter.Integrations;
using CNPJExporter.Modules.Rntrc.Configuration;
using CNPJExporter.Modules.Rntrc.Downloaders;
using CNPJExporter.Modules.Rntrc.Processors;

namespace CNPJExporter.Modules.Rntrc;

public sealed class DataIntegration : IDataIntegration
{
    private readonly IntegrationOptions _options;
    private readonly Downloader _downloader;
    private readonly ParquetProcessor _processor;

    public DataIntegration(IntegrationOptions options)
    {
        _options = options;
        _downloader = new Downloader(options);
        _processor = new ParquetProcessor();
        Descriptor = new DataIntegrationDescriptor(
            Key: "rntrc",
            JsonPropertyName: "rntrc",
            RefreshInterval: TimeSpan.FromHours(Math.Max(1, options.RefreshHours)),
            SchemaVersion: "1");
    }

    public DataIntegrationDescriptor Descriptor { get; }

    public async Task<DataIntegrationRunResult> RunAsync(
        DataIntegrationRunContext context,
        CancellationToken cancellationToken = default)
    {
        Descriptor.Validate();
        Directory.CreateDirectory(context.ModuleWorkDir);
        Directory.CreateDirectory(context.ModuleParquetDir);

        var source = await _downloader.GetSourceFileAsync(cancellationToken);
        var csvPath = await _downloader.DownloadIfNeededAsync(source, context.ModuleWorkDir, cancellationToken);
        var parquetPath = Path.Combine(context.ModuleParquetDir, "rntrc.parquet");
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
