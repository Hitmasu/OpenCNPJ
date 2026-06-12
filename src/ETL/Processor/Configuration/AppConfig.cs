using System.Text.Json;

namespace CNPJExporter.Configuration;

public class AppConfig
{
    public const string BigQueryEnabledEnvironmentVariable = "OPENCNPJ_BIGQUERY_ENABLED";
    public const string BigQueryProjectIdEnvironmentVariable = "OPENCNPJ_BIGQUERY_PROJECT_ID";

    public PathsConfig Paths { get; set; } = new();
    public RcloneSettings Rclone { get; set; } = new();
    public DuckDbSettings DuckDb { get; set; } = new();
    public ShardSettings Shards { get; set; } = new();
    public DownloaderSettings Downloader { get; set; } = new();
    public CnoIntegrationSettings CnoIntegration { get; set; } = new();
    public RntrcIntegrationSettings RntrcIntegration { get; set; } = new();
    public BigQuerySettings BigQuery { get; set; } = new();

    public class PathsConfig
    {
        public string DataDir { get; set; } = string.Empty;
        public string ParquetDir { get; set; } = string.Empty;
        public string OutputDir { get; set; } = string.Empty;
        public string DownloadDir { get; set; } = string.Empty;
    }

    public class RcloneSettings
    {
        public string RemoteBase { get; set; } = string.Empty;
        public int Transfers { get; set; } = 0;
        public int MaxConcurrentUploads { get; set; } = 0;
        public string BufferSize { get; set; } = "16M";
        public int UploadVerificationRetries { get; set; } = 3;
        public int UploadVerificationDelaySeconds { get; set; } = 15;
    }

    public class DuckDbSettings
    {
        public bool UseInMemory { get; set; } = false;
        public int ThreadsPragma { get; set; } = 0;
        public string MemoryLimit { get; set; } = "4GB";
        public int EngineThreads { get; set; } = 0;
        public bool PreserveInsertionOrder { get; set; } = false;
        public int PartitionedWriteMaxOpenFiles { get; set; } = 16;
    }

    public class ShardSettings
    {
        public int PrefixLength { get; set; } = 3;
        public int MaxParallelProcessing { get; set; } = 0;
        public int QueryBatchSize { get; set; } = 4;
        public int QueryRangeFanOut { get; set; } = 5;
        public int QsaMaterializationRangeFanOut { get; set; } = 2;
        public string RemoteDir { get; set; } = "shards";
    }

    public class DownloaderSettings
    {
        public int ParallelDownloads { get; set; } = 0;
        public string PublicShareRoot { get; set; } = string.Empty;
    }

    public class CnoIntegrationSettings
    {
        public bool Enabled { get; set; } = true;
        public string PublicShareRoot { get; set; } = string.Empty;
        public string ZipFileName { get; set; } = "cno.zip";
        public int RefreshHours { get; set; } = 24;
    }

    public class RntrcIntegrationSettings
    {
        public bool Enabled { get; set; } = true;
        public string PackageShowUrl { get; set; } = string.Empty;
        public int RefreshHours { get; set; } = 24;
    }

    public class BigQuerySettings
    {
        public bool Enabled { get; set; } = false;
        public string ProjectId { get; set; } = string.Empty;
        public string Dataset { get; set; } = string.Empty;
        public string TablePrefix { get; set; } = string.Empty;
        public string Location { get; set; } = string.Empty;
        public string BqExecutable { get; set; } = "bq";
        public bool KeepStagingTables { get; set; } = false;
    }

    public static AppConfig Current { get; private set; } = new();

    public static AppConfig Load(string? path = null)
    {
        var configPath = path ?? Path.Combine(Environment.CurrentDirectory, "config.json");
        try
        {
            if (File.Exists(configPath))
            {
                var json = File.ReadAllText(configPath);
                var cfg = JsonSerializer.Deserialize<AppConfig>(json, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });
                if (cfg != null)
                {
                    Current = ApplyEnvironmentOverrides(cfg);
                    return Current;
                }
            }
        }
        catch
        {
        }
        Current = ApplyEnvironmentOverrides(new());
        return Current;
    }

    private static AppConfig ApplyEnvironmentOverrides(AppConfig config)
    {
        var bigQueryEnabled = Environment.GetEnvironmentVariable(BigQueryEnabledEnvironmentVariable);
        if (!string.IsNullOrWhiteSpace(bigQueryEnabled))
            config.BigQuery.Enabled = ParseBooleanEnvironmentValue(bigQueryEnabled, BigQueryEnabledEnvironmentVariable);

        var bigQueryProjectId = Environment.GetEnvironmentVariable(BigQueryProjectIdEnvironmentVariable);
        if (!string.IsNullOrWhiteSpace(bigQueryProjectId))
            config.BigQuery.ProjectId = bigQueryProjectId.Trim();

        return config;
    }

    private static bool ParseBooleanEnvironmentValue(string value, string name)
    {
        return value.Trim().ToLowerInvariant() switch
        {
            "true" => true,
            "false" => false,
            _ => throw new InvalidOperationException($"{name} deve ser true ou false.")
        };
    }
}
