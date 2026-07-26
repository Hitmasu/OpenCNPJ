namespace CNPJExporter.Modules.PortalTransparencia.Configuration;

public sealed class IntegrationOptions
{
    public bool Enabled { get; set; } = true;
    public string CatalogBaseUrl { get; set; } = string.Empty;
    public string[] EnabledDatasets { get; set; } = [];
    public int ShardPrefixLength { get; set; } = 3;
    public int DuckDbThreads { get; set; } = 1;
    public string DuckDbMemoryLimit { get; set; } = "512MB";
    public string DuckDbMaxTempDirectorySize { get; set; } = "20GB";
    public int ProcessingPartitions { get; set; } = 64;
}
