namespace CNPJExporter.Integrations;

public interface IDataIntegrationStateStore
{
    Task<DataIntegrationHashState> LoadAsync(
        DataIntegrationDescriptor descriptor,
        CancellationToken cancellationToken = default);

    Task SaveAsync(
        DataIntegrationDescriptor descriptor,
        DataIntegrationHashState state,
        CancellationToken cancellationToken = default);
}
