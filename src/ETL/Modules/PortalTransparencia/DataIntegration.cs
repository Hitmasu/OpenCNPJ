using CNPJExporter.Integrations;
using CNPJExporter.Modules.PortalTransparencia.Configuration;
using CNPJExporter.Modules.PortalTransparencia.Downloaders;
using CNPJExporter.Modules.PortalTransparencia.Models;
using CNPJExporter.Modules.PortalTransparencia.Processors;

namespace CNPJExporter.Modules.PortalTransparencia;

public sealed class DataIntegration : IDataIntegration, IDataIntegrationSourceProvider
{
    private readonly IntegrationOptions _options;
    private readonly PortalDatasetDefinition _definition;
    private readonly Downloader _downloader;
    private readonly ParquetProcessor _processor;
    private readonly PartitionedParquetProcessor _partitionedProcessor;

    private DataIntegration(
        IntegrationOptions options,
        PortalDatasetDefinition definition)
    {
        _options = options;
        _definition = definition;
        _downloader = new Downloader(options, definition);
        _processor = new ParquetProcessor(options);
        _partitionedProcessor = new PartitionedParquetProcessor(options);
        Descriptor = new DataIntegrationDescriptor(
            Key: definition.Key,
            JsonPropertyName: definition.Key,
            RefreshInterval: definition.RefreshInterval,
            SchemaVersion: "1",
            SegmentCollectionProperty: definition.SegmentCollectionProperty);
    }

    public DataIntegrationDescriptor Descriptor { get; }

    public static IReadOnlyList<IDataIntegration> CreateEnabled(IntegrationOptions options)
    {
        if (!options.Enabled)
            return [];

        return PortalDatasetDefinition
            .ResolveEnabled(options.EnabledDatasets)
            .Select(definition => (IDataIntegration)new DataIntegration(options, definition))
            .ToArray();
    }

    public Task<SourceFile> GetSourceAsync(CancellationToken cancellationToken = default) =>
        _downloader.GetSourceAsync(cancellationToken);

    public async Task<DataIntegrationRunResult> RunAsync(
        DataIntegrationRunContext context,
        CancellationToken cancellationToken = default)
    {
        Descriptor.Validate();
        Directory.CreateDirectory(context.ModuleWorkDir);
        Directory.CreateDirectory(context.ModuleParquetDir);

        var source = context.Source
                     ?? throw new InvalidOperationException(
                         $"{_definition.Key} requer Source no DataIntegrationRunContext.");

        if (_definition.IsSegmented)
        {
            return await RunSegmentedAsync(
                context,
                source,
                cancellationToken);
        }

        var extracted = await _downloader.DownloadAndExtractAsync(
            source,
            context.ModuleWorkDir,
            cancellationToken);
        var moduleUpdatedAt = source.LastModified ?? context.Now;

        if (_definition.Key is "convenios" or "emendas_parlamentares")
        {
            var result = await _partitionedProcessor.ConvertAsync(
                _definition,
                extracted,
                Path.Combine(context.ModuleParquetDir, $"{_definition.Key}-parts"),
                moduleUpdatedAt,
                Math.Max(1, _options.ShardPrefixLength),
                cancellationToken);
            return new DataIntegrationRunResult(
                source.SourceVersion,
                moduleUpdatedAt,
                result.ParquetGlob,
                result.RecordCount,
                new Dictionary<string, string>(StringComparer.Ordinal));
        }

        var parquetPath = Path.Combine(
            context.ModuleParquetDir,
            $"{_definition.Key}.parquet");
        await _processor.ConvertToParquetAsync(
            _definition,
            extracted,
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

    private async Task<DataIntegrationRunResult> RunSegmentedAsync(
        DataIntegrationRunContext context,
        SourceFile source,
        CancellationToken cancellationToken)
    {
        var artifactSegments = await _downloader.GetHistoricalSegmentsAsync(
            context.Now.Year,
            cancellationToken);
        var previousSegments = context.PreviousState.EffectiveSegments
            .ToDictionary(segment => segment.Id, StringComparer.Ordinal);
        var segments = new List<DataIntegrationSegment>(
            artifactSegments.Count);

        foreach (var artifactSegment in artifactSegments)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var segmentSourceVersion = Downloader.BuildSegmentSourceVersion(
                _definition.Key,
                artifactSegment,
                source);
            if (previousSegments.TryGetValue(
                    artifactSegment.Id,
                    out var previous)
                && string.Equals(
                    previous.SourceVersion,
                    segmentSourceVersion,
                    StringComparison.Ordinal)
                && ParquetGlobExists(previous.ParquetGlob))
            {
                segments.Add(previous);
                continue;
            }

            var outputDirectory = Path.Combine(
                context.ModuleParquetDir,
                "segments",
                artifactSegment.Id,
                segmentSourceVersion);
            var completed = await PartitionedParquetProcessor.TryLoadCompletedAsync(
                outputDirectory,
                cancellationToken);
            if (completed is { RecordCount: > 0 })
            {
                segments.Add(
                    new DataIntegrationSegment(
                        artifactSegment.Id,
                        segmentSourceVersion,
                        completed.UpdatedAt,
                        completed.ParquetGlob,
                        completed.RecordCount,
                        artifactSegment.ReplacesSegmentIds));
                continue;
            }

            var extractedArtifacts = new List<ExtractedDataset>(
                artifactSegment.Artifacts.Count);
            foreach (var artifact in artifactSegment.Artifacts)
            {
                extractedArtifacts.Add(
                    await _downloader.DownloadAndExtractArtifactAsync(
                        artifact,
                        source,
                        context.ModuleWorkDir,
                        cancellationToken));
            }

            var updatedAt =
                artifactSegment.Artifacts.Any(
                    artifact => artifact.DownloadUri == source.Uri)
                    ? source.LastModified ?? context.Now
                    : context.Now;
            var processed = await _partitionedProcessor.ConvertAsync(
                _definition,
                ExtractedDataset.Combine(extractedArtifacts),
                outputDirectory,
                updatedAt,
                Math.Max(1, _options.ShardPrefixLength),
                cancellationToken);
            if (processed.RecordCount == 0)
                continue;

            segments.Add(
                new DataIntegrationSegment(
                    artifactSegment.Id,
                    segmentSourceVersion,
                    updatedAt,
                    processed.ParquetGlob,
                    processed.RecordCount,
                    artifactSegment.ReplacesSegmentIds));
        }

        var orderedSegments = segments
            .OrderBy(segment => segment.Id, StringComparer.Ordinal)
            .ToArray();
        return new DataIntegrationRunResult(
            source.SourceVersion,
            source.LastModified ?? context.Now,
            null,
            orderedSegments.Sum(segment => segment.RecordCount),
            new Dictionary<string, string>(StringComparer.Ordinal),
            orderedSegments);
    }

    private static bool ParquetGlobExists(string parquetGlob)
    {
        var directory = Path.GetDirectoryName(parquetGlob);
        return directory is not null
               && Directory.Exists(directory)
               && Directory.EnumerateFiles(
                       directory,
                       "*.parquet",
                       SearchOption.TopDirectoryOnly)
                   .Any();
    }
}
