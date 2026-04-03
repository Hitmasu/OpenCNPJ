using CNPJExporter.Configuration;
using CNPJExporter.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class LocalArtifactCleanerTests
{
    [TestMethod]
    public async Task CleanupDatasetArtifactsAsync_ShouldRemove_DatasetDirectories_AndLocalEngineArtifacts()
    {
        var root = Path.Combine(Path.GetTempPath(), $"opencnpj-cleanup-{Guid.NewGuid():N}");
        var processorDir = Path.Combine(root, "processor");
        Directory.CreateDirectory(processorDir);

        var configPath = Path.Combine(root, "config.json");
        var datasetKey = "2026-03";

        try
        {
            var downloadDir = Path.Combine(processorDir, "downloads", datasetKey);
            var dataDir = Path.Combine(processorDir, "extracted_data", datasetKey);
            var parquetDir = Path.Combine(processorDir, "parquet_data", datasetKey);
            var outputDir = Path.Combine(processorDir, "cnpj_shards", datasetKey);
            var tempDir = Path.Combine(processorDir, "temp");
            var hashCacheDir = Path.Combine(processorDir, "hash_cache");
            var duckDbPath = Path.Combine(processorDir, "cnpj.duckdb");
            var workerAssetsDir = Path.Combine(root, "worker-assets");

            foreach (var directory in new[] { downloadDir, dataDir, parquetDir, outputDir, tempDir, hashCacheDir, workerAssetsDir })
            {
                Directory.CreateDirectory(directory);
                await File.WriteAllTextAsync(Path.Combine(directory, "marker.txt"), "x");
            }

            await File.WriteAllTextAsync(duckDbPath, "db");

            await File.WriteAllTextAsync(
                configPath,
                $$"""
                {
                  "Paths": {
                    "DownloadDir": "{{Path.Combine(processorDir, "downloads").Replace("\\", "\\\\")}}",
                    "DataDir": "{{Path.Combine(processorDir, "extracted_data").Replace("\\", "\\\\")}}",
                    "ParquetDir": "{{Path.Combine(processorDir, "parquet_data").Replace("\\", "\\\\")}}",
                    "OutputDir": "{{Path.Combine(processorDir, "cnpj_shards").Replace("\\", "\\\\")}}",
                    "WorkerAssetsDir": "{{workerAssetsDir.Replace("\\", "\\\\")}}"
                  },
                  "DuckDb": {
                    "UseInMemory": false
                  }
                }
                """);

            AppConfig.Load(configPath);

            var originalCwd = Environment.CurrentDirectory;
            Environment.CurrentDirectory = processorDir;
            try
            {
                await LocalArtifactCleaner.CleanupDatasetArtifactsAsync(datasetKey);
            }
            finally
            {
                Environment.CurrentDirectory = originalCwd;
            }

            Assert.IsFalse(Directory.Exists(downloadDir));
            Assert.IsFalse(Directory.Exists(dataDir));
            Assert.IsFalse(Directory.Exists(parquetDir));
            Assert.IsFalse(Directory.Exists(outputDir));
            Assert.IsFalse(Directory.Exists(tempDir));
            Assert.IsFalse(Directory.Exists(hashCacheDir));
            Assert.IsFalse(File.Exists(duckDbPath));
            Assert.IsTrue(Directory.Exists(workerAssetsDir), "Assets do Worker não devem ser apagados pelo cleanup.");
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, true);
        }
    }
}
