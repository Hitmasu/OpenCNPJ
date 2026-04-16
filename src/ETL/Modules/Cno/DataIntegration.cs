using CNPJExporter.Integrations;
using CNPJExporter.Modules.Cno.Configuration;
using CNPJExporter.Modules.Cno.Downloaders;
using CNPJExporter.Modules.Cno.Processors;

namespace CNPJExporter.Modules.Cno;

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
            Key: "cno",
            JsonPropertyName: "cno",
            RefreshInterval: TimeSpan.FromHours(Math.Max(1, options.RefreshHours)),
            SchemaVersion: "2");
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
        var zipPath = await _downloader.DownloadIfNeededAsync(source, context.ModuleWorkDir, cancellationToken);

        var extractDir = Path.Combine(context.ModuleWorkDir, "extracted", ToPathSegment(source.SourceVersion));
        var extractedFiles = await _processor.ExtractAsync(zipPath, extractDir, cancellationToken);
        var parquetPath = Path.Combine(context.ModuleParquetDir, "cno.parquet");
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
