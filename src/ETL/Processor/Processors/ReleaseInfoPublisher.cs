using System.Text;
using System.Text.Json;
using CNPJExporter.Exporters;
using CNPJExporter.Processors.Models;
using CNPJExporter.Utils;
using Spectre.Console;

namespace CNPJExporter.Processors;

internal sealed class ReleaseInfoPublisher
{
    private static readonly UTF8Encoding Utf8NoBom = new(false);

    public async Task PublishAsync(
        ReleaseInfoPublication publication,
        string outputRootDir,
        CancellationToken cancellationToken = default)
    {
        var json = Serialize(publication);

        var outputDir = Path.Combine(
            DatasetPathResolver.GetDatasetPath(outputRootDir, publication.DatasetKey),
            "releases",
            publication.PublicationReleaseId.Trim('/'));
        Directory.CreateDirectory(outputDir);
        var localInfoPath = Path.Combine(outputDir, "info.json");
        await File.WriteAllTextAsync(localInfoPath, json, Utf8NoBom, cancellationToken);

        AnsiConsole.MarkupLine("[cyan]📤 Enviando info.json para Storage...[/]");
        var ok = await RcloneClient.UploadFileAsync(localInfoPath, "info.json");
        if (!ok)
            throw new InvalidOperationException("Falha ao enviar info.json para Storage.");

        AnsiConsole.MarkupLine("[green]✓ info.json enviado para Storage[/]");
    }

    internal static string SerializeForTest(ReleaseInfoPublication publication) => Serialize(publication);

    private static string Serialize(ReleaseInfoPublication publication)
    {
        var payload = new
        {
            total = publication.Total,
            last_updated = publication.LastUpdated,
            shard_prefix_length = publication.ShardPrefixLength,
            shard_count = publication.ShardCount,
            storage_release_id = publication.StorageReleaseId,
            datasets = BuildDatasetsInfo(publication),
            shard_index_distribution = "r2",
            shard_format = "ndjson+binary-index",
            zip_layout = "per-dataset-shards-v1",
            cnpj_type = "string"
        };

        return JsonSerializer.Serialize(payload, new JsonSerializerOptions
        {
            PropertyNamingPolicy = null,
            WriteIndented = false
        });
    }

    private static object BuildDatasetsInfo(ReleaseInfoPublication publication)
    {
        var datasets = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            [publication.ReceitaDatasetKey] = new
            {
                storage_release_id = publication.StorageReleaseId,
                updated_at = publication.LastUpdated,
                record_count = publication.Total,
                zip_available = publication.BaseZip.Available,
                zip_size = publication.BaseZip.Size,
                zip_url = publication.BaseZip.Url,
                zip_md5checksum = publication.BaseZip.Md5Checksum
            }
        };

        foreach (var (key, module) in publication.ModuleShards.OrderBy(kvp => kvp.Key, StringComparer.Ordinal))
        {
            datasets[key] = new
            {
                json_property_name = module.JsonPropertyName,
                storage_release_id = module.StorageReleaseId,
                schema_version = module.SchemaVersion,
                source_version = module.SourceVersion,
                updated_at = module.UpdatedAt.ToString("o"),
                record_count = module.RecordCount,
                zip_available = module.Zip.Available,
                zip_size = module.Zip.Size,
                zip_url = module.Zip.Url,
                zip_md5checksum = module.Zip.Md5Checksum
            };
        }

        return datasets;
    }
}
