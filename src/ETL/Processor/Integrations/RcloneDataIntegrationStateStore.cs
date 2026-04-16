using System.Text.Json;
using CNPJExporter.Exporters;

namespace CNPJExporter.Integrations;

public sealed class RcloneDataIntegrationStateStore : IDataIntegrationStateStore
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false
    };

    private readonly string _localStateRoot;

    public RcloneDataIntegrationStateStore(string localStateRoot)
    {
        _localStateRoot = localStateRoot;
    }

    public async Task<DataIntegrationHashState> LoadAsync(
        DataIntegrationDescriptor descriptor,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var localPath = GetLocalStatePath(descriptor);
        Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);

        var downloaded = await RcloneClient.DownloadFileAsync(GetRemoteStatePath(descriptor), localPath);
        if (!downloaded || !File.Exists(localPath))
            return DataIntegrationHashState.Empty;

        await using var stream = File.OpenRead(localPath);
        var state = await JsonSerializer.DeserializeAsync<DataIntegrationHashStateDto>(
            stream,
            JsonOptions,
            cancellationToken);

        return new DataIntegrationHashState(
            new Dictionary<string, string>(state?.Hashes ?? [], StringComparer.Ordinal),
            state?.SourceVersion,
            state?.UpdatedAt,
            state?.ParquetGlob,
            state?.SchemaVersion);
    }

    public async Task SaveAsync(
        DataIntegrationDescriptor descriptor,
        DataIntegrationHashState state,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var localPath = GetLocalStatePath(descriptor);
        Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);

        var dto = new DataIntegrationHashStateDto(
            state.SourceVersion,
            state.UpdatedAt,
            state.ParquetGlob,
            state.SchemaVersion,
            new Dictionary<string, string>(state.Hashes, StringComparer.Ordinal));

        await using (var stream = File.Create(localPath))
        {
            await JsonSerializer.SerializeAsync(stream, dto, JsonOptions, cancellationToken);
        }

        var uploaded = await RcloneClient.UploadFileAsync(localPath, GetRemoteStatePath(descriptor));
        if (!uploaded)
            throw new InvalidOperationException($"Falha ao publicar hashtable da integração {descriptor.Key}.");
    }

    private string GetLocalStatePath(DataIntegrationDescriptor descriptor) =>
        Path.Combine(_localStateRoot, descriptor.Key, "hashes.json");

    private static string GetRemoteStatePath(DataIntegrationDescriptor descriptor) =>
        $"integrations/state/{descriptor.Key}/hashes.json";

    private sealed record DataIntegrationHashStateDto(
        string? SourceVersion,
        DateTimeOffset? UpdatedAt,
        string? ParquetGlob,
        string? SchemaVersion,
        Dictionary<string, string> Hashes);
}
