namespace CNPJExporter.Integrations;

public interface IDataIntegration
{
    DataIntegrationDescriptor Descriptor { get; }

    Task<DataIntegrationRunResult> RunAsync(
        DataIntegrationRunContext context,
        CancellationToken cancellationToken = default);
}
