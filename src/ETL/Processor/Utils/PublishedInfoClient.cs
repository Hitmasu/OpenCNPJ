using System.Text.Json;
using CNPJExporter.Exporters;
using CNPJExporter.Processors.Models;

namespace CNPJExporter.Utils;

internal sealed class PublishedInfoClient
{
    private readonly IPublishedInfoReader _reader;

    public PublishedInfoClient()
    {
        _reader = new RclonePublishedInfoReader();
    }

    internal PublishedInfoClient(IPublishedInfoReader reader)
    {
        _reader = reader;
    }

    internal Type ReaderTypeForTest => _reader.GetType();

    public async Task<string?> GetPublishedLastUpdatedAsync(CancellationToken cancellationToken = default)
    {
        var info = await GetPublishedInfoAsync(cancellationToken);
        return info.LastUpdated;
    }

    public async Task<string?> GetPublishedReleaseIdAsync(CancellationToken cancellationToken = default)
    {
        var info = await GetPublishedInfoAsync(cancellationToken);
        return info.StorageReleaseId;
    }

    public async Task<PublishedInfoSnapshot> GetPublishedInfoAsync(CancellationToken cancellationToken = default)
    {
        using var document = await GetPublishedInfoDocumentAsync(cancellationToken);
        var root = document.RootElement;
        var total = TryGetLong(root, "total");
        var shardCount = TryGetInt(root, "shard_count");
        var lastUpdated = TryGetString(root, "last_updated");
        var storageReleaseId = TryGetString(root, "storage_release_id");
        var baseZip = PublishedZipArtifactSnapshot.Missing;
        var moduleShards = new Dictionary<string, PublishedModuleShardSnapshot>(StringComparer.Ordinal);

        if (root.TryGetProperty("datasets", out var datasetsElement)
            && datasetsElement.ValueKind == JsonValueKind.Object)
        {
            foreach (var moduleProperty in datasetsElement.EnumerateObject())
            {
                if (string.Equals(moduleProperty.Name, "receita", StringComparison.Ordinal))
                {
                    if (moduleProperty.Value.ValueKind == JsonValueKind.Object)
                        baseZip = TryGetZipSnapshot(moduleProperty.Value);

                    continue;
                }

                if (moduleProperty.Value.ValueKind != JsonValueKind.Object)
                    continue;

                var moduleElement = moduleProperty.Value;
                var moduleStorageReleaseId = TryGetString(moduleElement, "storage_release_id");
                var routingReleaseId = TryGetString(moduleElement, "routing_release_id");
                var segments = ParseSegments(moduleElement);

                if (string.IsNullOrWhiteSpace(moduleStorageReleaseId)
                    && string.IsNullOrWhiteSpace(routingReleaseId))
                {
                    continue;
                }

                moduleShards[moduleProperty.Name] = new PublishedModuleShardSnapshot(
                    moduleProperty.Name,
                    TryGetString(moduleElement, "json_property_name") ?? moduleProperty.Name,
                    TryGetString(moduleElement, "schema_version") ?? "",
                    TryGetString(moduleElement, "source_version"),
                    TryGetDateTimeOffset(moduleElement, "updated_at") ?? DateTimeOffset.MinValue,
                    TryGetLong(moduleElement, "record_count") ?? 0,
                    moduleStorageReleaseId ?? routingReleaseId!,
                    TryGetZipSnapshot(moduleElement),
                    routingReleaseId,
                    TryGetString(moduleElement, "segment_collection_property"),
                    segments);
            }
        }

        return new PublishedInfoSnapshot(
            total,
            shardCount,
            lastUpdated,
            storageReleaseId,
            baseZip,
            moduleShards);
    }

    private async Task<JsonDocument> GetPublishedInfoDocumentAsync(CancellationToken cancellationToken)
    {
        await using var stream = await _reader.OpenReadAsync(cancellationToken);
        return await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
    }

    private static string? TryGetString(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property)
            || property.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        return property.GetString();
    }

    private static long? TryGetLong(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property)
            || property.ValueKind != JsonValueKind.Number
            || !property.TryGetInt64(out var value))
        {
            return null;
        }

        return value;
    }

    private static int? TryGetInt(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property)
            || property.ValueKind != JsonValueKind.Number
            || !property.TryGetInt32(out var value))
        {
            return null;
        }

        return value;
    }

    private static bool? TryGetBool(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property)
            || (property.ValueKind != JsonValueKind.True && property.ValueKind != JsonValueKind.False))
        {
            return null;
        }

        return property.GetBoolean();
    }

    private static DateTimeOffset? TryGetDateTimeOffset(JsonElement element, string propertyName)
    {
        var value = TryGetString(element, propertyName);
        if (string.IsNullOrWhiteSpace(value)
            || !DateTimeOffset.TryParse(value, out var dateTimeOffset))
        {
            return null;
        }

        return dateTimeOffset;
    }

    private static PublishedZipArtifactSnapshot TryGetZipSnapshot(JsonElement element)
    {
        var available = TryGetBool(element, "zip_available") ?? false;
        var size = TryGetLong(element, "zip_size") ?? 0;
        var url = TryGetString(element, "zip_url") ?? "";
        var md5Checksum = TryGetString(element, "zip_md5checksum") ?? "";

        return new PublishedZipArtifactSnapshot(
            available,
            size,
            url,
            md5Checksum);
    }

    private static IReadOnlyList<PublishedModuleSegmentSnapshot> ParseSegments(
        JsonElement moduleElement)
    {
        if (!moduleElement.TryGetProperty("segments", out var segmentsElement)
            || segmentsElement.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var segments = new List<PublishedModuleSegmentSnapshot>();
        foreach (var segmentElement in segmentsElement.EnumerateArray())
        {
            if (segmentElement.ValueKind != JsonValueKind.Object)
                continue;

            var id = TryGetString(segmentElement, "id");
            var storageReleaseId = TryGetString(
                segmentElement,
                "storage_release_id");
            if (string.IsNullOrWhiteSpace(id)
                || string.IsNullOrWhiteSpace(storageReleaseId))
            {
                continue;
            }

            segments.Add(new PublishedModuleSegmentSnapshot(
                id,
                TryGetString(segmentElement, "source_version"),
                TryGetDateTimeOffset(segmentElement, "updated_at")
                    ?? DateTimeOffset.MinValue,
                TryGetLong(segmentElement, "record_count") ?? 0,
                storageReleaseId,
                TryGetZipSnapshot(segmentElement)));
        }

        return segments;
    }

    internal interface IPublishedInfoReader
    {
        Task<Stream> OpenReadAsync(CancellationToken cancellationToken);
    }

    internal sealed class RclonePublishedInfoReader : IPublishedInfoReader
    {
        public async Task<Stream> OpenReadAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var localPath = Path.Combine(Path.GetTempPath(), $"opencnpj-info-{Guid.NewGuid():N}.json");
            try
            {
                var downloaded = await RcloneClient.DownloadFileAsync("info.json", localPath);
                if (!downloaded || !File.Exists(localPath))
                    throw new InvalidOperationException("Não foi possível baixar info.json do storage via rclone.");

                var bytes = await File.ReadAllBytesAsync(localPath, cancellationToken);
                return new MemoryStream(bytes, writable: false);
            }
            finally
            {
                if (File.Exists(localPath))
                    File.Delete(localPath);
            }
        }
    }

}
