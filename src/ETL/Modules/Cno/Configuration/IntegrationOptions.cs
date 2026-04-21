namespace CNPJExporter.Modules.Cno.Configuration;

public sealed class IntegrationOptions
{
    public bool Enabled { get; init; } = true;
    public string PublicShareRoot { get; init; } = string.Empty;
    public string ZipFileName { get; init; } = "cno.zip";
    public int RefreshHours { get; init; } = 24;
    public int ShardPrefixLength { get; init; } = 3;
}
