using CNPJExporter.Configuration;
using CNPJExporter.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class WorkerAssetStagerTests
{
    [TestMethod]
    public async Task StageAsync_ShouldCopy_Info_And_IndexFiles_Only()
    {
        var root = Path.Combine(Path.GetTempPath(), $"opencnpj-worker-assets-{Guid.NewGuid():N}");
        var outputDir = Path.Combine(root, "output");
        var workerAssetsDir = Path.Combine(root, "worker-assets");
        var datasetKey = "2026-03";
        var datasetOutputDir = Path.Combine(outputDir, datasetKey);
        var shardDir = Path.Combine(datasetOutputDir, "shards");
        var configPath = Path.Combine(root, "config.json");

        try
        {
            Directory.CreateDirectory(shardDir);
            await File.WriteAllTextAsync(Path.Combine(datasetOutputDir, "info.json"), "{\"ok\":true}");
            await File.WriteAllTextAsync(Path.Combine(shardDir, "123.index.json"), "{\"prefix\":\"123\"}");
            await File.WriteAllTextAsync(Path.Combine(shardDir, "123.ndjson"), "{\"cnpj\":\"123\",\"data\":{}}\n");

            Directory.CreateDirectory(workerAssetsDir);
            await File.WriteAllTextAsync(Path.Combine(workerAssetsDir, "stale.txt"), "old");

            await File.WriteAllTextAsync(
                configPath,
                $$"""
                {
                  "Paths": {
                    "OutputDir": "{{outputDir.Replace("\\", "\\\\")}}",
                    "WorkerAssetsDir": "{{workerAssetsDir.Replace("\\", "\\\\")}}"
                  },
                  "Shards": {
                    "RemoteDir": "shards"
                  }
                }
                """);

            AppConfig.Load(configPath);

            var stagedRoot = await WorkerAssetStager.StageAsync(datasetKey);

            Assert.AreEqual(Path.GetFullPath(workerAssetsDir), stagedRoot);
            Assert.IsTrue(File.Exists(Path.Combine(workerAssetsDir, "files", "info.json")));
            Assert.IsTrue(File.Exists(Path.Combine(workerAssetsDir, "files", "shards", "123.index.json")));
            Assert.IsFalse(File.Exists(Path.Combine(workerAssetsDir, "files", "shards", "123.ndjson")));
            Assert.IsFalse(File.Exists(Path.Combine(workerAssetsDir, "stale.txt")));
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, true);
        }
    }
}
