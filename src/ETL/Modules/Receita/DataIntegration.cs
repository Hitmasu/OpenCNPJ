using CNPJExporter.Integrations;

namespace CNPJExporter.Modules.Receita;

public sealed class DataIntegration
{
    public const string Key = "receita";

    public DataIntegration()
    {
        Descriptor = new DataIntegrationDescriptor(
            Key: Key,
            JsonPropertyName: Key,
            RefreshInterval: TimeSpan.FromDays(30),
            SchemaVersion: "1");
    }

    public DataIntegrationDescriptor Descriptor { get; }
}
