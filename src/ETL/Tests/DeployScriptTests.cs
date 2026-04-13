using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class DeployScriptTests
{
    [TestMethod]
    public void CleanupOnSuccess_ShouldRunAfterWorkerAssetsAreCopied()
    {
        var script = File.ReadAllText(FindDeployScript());

        Assert.IsFalse(
            script.Contains("PIPELINE_ARGS+=(--cleanup-on-success)", StringComparison.Ordinal),
            "deploy.sh must not pass --cleanup-on-success to the ETL pipeline before Worker assets are copied.");

        var copyIndex = script.IndexOf("copy_worker_assets \"$DATASET_KEY\" \"$RELEASE_ID\"", StringComparison.Ordinal);
        var cleanupIndex = script.IndexOf("cleanup_dataset_artifacts \"$DATASET_KEY\"", StringComparison.Ordinal);

        Assert.IsTrue(copyIndex >= 0, "deploy.sh must copy Worker assets.");
        Assert.IsTrue(cleanupIndex > copyIndex, "dataset cleanup must happen only after Worker assets are copied.");
    }

    [TestMethod]
    public void DeployWorker_ShouldNotRequireRipgrepToParsePublishedUrl()
    {
        var script = File.ReadAllText(FindDeployScript());

        Assert.IsFalse(
            script.Contains(" | rg ", StringComparison.Ordinal) ||
            script.Contains("$(rg ", StringComparison.Ordinal) ||
            script.Contains(" rg -", StringComparison.Ordinal),
            "deploy.sh runs inside the Docker image and must not depend on ripgrep for Worker URL parsing.");
    }

    private static string FindDeployScript()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null)
        {
            var candidate = Path.Combine(current.FullName, "src", "scripts", "deploy.sh");
            if (File.Exists(candidate))
                return candidate;

            current = current.Parent;
        }

        throw new FileNotFoundException("Could not locate src/scripts/deploy.sh.");
    }
}
