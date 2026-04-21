namespace CNPJExporter.Modules.Rntrc.Configuration;

public sealed class IntegrationOptions
{
    public bool Enabled { get; set; } = true;
    public string PackageShowUrl { get; set; } = string.Empty;
    public int RefreshHours { get; set; } = 24;
    public int ShardPrefixLength { get; set; } = 3;
}
