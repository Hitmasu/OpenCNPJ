using System.Text.Json;

namespace CNPJExporter.Configuration;

public class AppConfig
{
    public PathsConfig Paths { get; set; } = new();
    public RcloneSettings Rclone { get; set; } = new();
    public DuckDbSettings DuckDb { get; set; } = new();
    public ShardSettings Shards { get; set; } = new();
    public DownloaderSettings Downloader { get; set; } = new();

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
        public string RemoteDir { get; set; } = "shards";
    }

    public class DownloaderSettings
    {
        public int ParallelDownloads { get; set; } = 0;
        public string PublicShareRoot { get; set; } = string.Empty;
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
                    Current = cfg;
                    return Current;
                }
            }
        }
        catch
        {
        }
        Current = new();
        return Current;
    }
}
