using System.Text.Json;

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
        using var response = await _httpClient.GetAsync(DatasetPublicationPolicy.PublishedInfoUri, cancellationToken);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);

        if (!document.RootElement.TryGetProperty("last_updated", out var lastUpdatedElement))
            return null;

        return lastUpdatedElement.GetString();
    }
}
