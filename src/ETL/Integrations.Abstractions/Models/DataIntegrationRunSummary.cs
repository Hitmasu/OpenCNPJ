namespace CNPJExporter.Integrations;

public sealed record DataIntegrationRunSummary(
    DataIntegrationDescriptor Descriptor,
    string? SourceVersion,
    DateTimeOffset UpdatedAt,
    string? ParquetGlob,
    long RecordCount,
    IReadOnlyList<string> ChangedCnpjs,
    IReadOnlyDictionary<string, string> CurrentHashes,
    bool RequiresFullPublish = false,
    DataIntegrationHashState? StateToPersist = null,
    bool ShouldPersistState = false,
    bool HasMetadataChanges = false)
{
    public bool HasChanges => ChangedCnpjs.Count > 0;

    public bool HasPublicationChanges => HasChanges || HasMetadataChanges || RequiresFullPublish;
}
