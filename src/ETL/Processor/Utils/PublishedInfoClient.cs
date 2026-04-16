using System.Text.Json;
using CNPJExporter.Processors.Models;

namespace CNPJExporter.Utils;

internal sealed class PublishedInfoClient
{
    private readonly HttpClient _httpClient;

    public PublishedInfoClient(HttpClient? httpClient = null)
    {
        _httpClient = httpClient ?? new HttpClient();
    }

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
        var defaultShardReleaseId = TryGetString(root, "default_shard_release_id") ?? storageReleaseId;
        var shardReleases = new Dictionary<string, string>(StringComparer.Ordinal);
        var moduleShards = new Dictionary<string, PublishedModuleShardSnapshot>(StringComparer.Ordinal);

        if (root.TryGetProperty("shard_releases", out var shardReleasesElement)
            && shardReleasesElement.ValueKind == JsonValueKind.Object)
        {
            ReadReleaseMap(shardReleasesElement, shardReleases);
        }

        if (root.TryGetProperty("module_shards", out var moduleShardsElement)
            && moduleShardsElement.ValueKind == JsonValueKind.Object)
        {
            foreach (var moduleProperty in moduleShardsElement.EnumerateObject())
            {
                if (moduleProperty.Value.ValueKind != JsonValueKind.Object)
                    continue;

                var moduleElement = moduleProperty.Value;
                var moduleStorageReleaseId = TryGetString(moduleElement, "storage_release_id");
                var moduleDefaultShardReleaseId = TryGetString(moduleElement, "default_shard_release_id") ?? moduleStorageReleaseId;

                if (string.IsNullOrWhiteSpace(moduleStorageReleaseId)
                    || string.IsNullOrWhiteSpace(moduleDefaultShardReleaseId))
                {
                    continue;
                }

                var moduleReleaseMap = new Dictionary<string, string>(StringComparer.Ordinal);
                if (moduleElement.TryGetProperty("shard_releases", out var moduleShardReleasesElement)
                    && moduleShardReleasesElement.ValueKind == JsonValueKind.Object)
                {
                    ReadReleaseMap(moduleShardReleasesElement, moduleReleaseMap);
                }

                moduleShards[moduleProperty.Name] = new PublishedModuleShardSnapshot(
                    moduleProperty.Name,
                    TryGetString(moduleElement, "json_property_name") ?? moduleProperty.Name,
                    TryGetString(moduleElement, "schema_version") ?? "",
                    TryGetString(moduleElement, "source_version"),
                    TryGetDateTimeOffset(moduleElement, "updated_at") ?? DateTimeOffset.MinValue,
                    TryGetLong(moduleElement, "record_count") ?? 0,
                    moduleStorageReleaseId,
                    moduleDefaultShardReleaseId,
                    moduleReleaseMap);
            }
        }

        return new PublishedInfoSnapshot(
            total,
            shardCount,
            lastUpdated,
            storageReleaseId,
            defaultShardReleaseId,
            shardReleases,
            moduleShards);
    }

    private async Task<JsonDocument> GetPublishedInfoDocumentAsync(CancellationToken cancellationToken)
    {
        using var response = await _httpClient.GetAsync(DatasetPublicationPolicy.PublishedInfoUri, cancellationToken);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
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

    private static void ReadReleaseMap(JsonElement element, IDictionary<string, string> target)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (property.Value.ValueKind != JsonValueKind.String)
                continue;

            var releaseId = property.Value.GetString();
            if (!string.IsNullOrWhiteSpace(releaseId))
                target[property.Name] = releaseId;
        }
    }
}
