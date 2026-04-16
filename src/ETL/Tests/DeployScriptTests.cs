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

    [TestMethod]
    public void CleanupDatasetArtifacts_ShouldPreserveParquetDirectory()
    {
        var script = File.ReadAllText(FindDeployScript());
        var cleanupFunctionIndex = script.IndexOf("cleanup_dataset_artifacts()", StringComparison.Ordinal);
        Assert.IsTrue(cleanupFunctionIndex >= 0, "deploy.sh must define cleanup_dataset_artifacts.");

        var cleanupFunction = script[cleanupFunctionIndex..];
        var nextFunctionIndex = cleanupFunction.IndexOf("\nvalidate_endpoint()", StringComparison.Ordinal);
        if (nextFunctionIndex >= 0)
            cleanupFunction = cleanupFunction[..nextFunctionIndex];

        Assert.IsFalse(
            cleanupFunction.Contains("\"ParquetDir\"", StringComparison.Ordinal),
            "cleanup_dataset_artifacts must preserve ParquetDir for incremental integration runs.");
        Assert.IsFalse(
            cleanupFunction.Contains("\"OutputDir\"", StringComparison.Ordinal),
            "cleanup_dataset_artifacts must preserve OutputDir for local release reuse.");
    }

    [TestMethod]
    public void CleanupDatasetArtifacts_ShouldRemoveIntegrationInputs_AndPreserveState()
    {
        var script = File.ReadAllText(FindDeployScript());
        var cleanupFunctionIndex = script.IndexOf("cleanup_dataset_artifacts()", StringComparison.Ordinal);
        Assert.IsTrue(cleanupFunctionIndex >= 0, "deploy.sh must define cleanup_dataset_artifacts.");

        var cleanupFunction = script[cleanupFunctionIndex..];
        var nextFunctionIndex = cleanupFunction.IndexOf("\nvalidate_endpoint()", StringComparison.Ordinal);
        if (nextFunctionIndex >= 0)
            cleanupFunction = cleanupFunction[..nextFunctionIndex];

        Assert.IsTrue(
            cleanupFunction.Contains("${data_dir}/integrations", StringComparison.Ordinal),
            "cleanup_dataset_artifacts must remove integration ZIP/CSV inputs.");
        Assert.IsTrue(
            cleanupFunction.Contains("! -name '_state'", StringComparison.Ordinal),
            "cleanup_dataset_artifacts must preserve integration hash state.");
    }

    [TestMethod]
    public void CopyWorkerAssets_ShouldIncludeModuleIndexes()
    {
        var script = File.ReadAllText(FindDeployScript());
        var copyFunctionIndex = script.IndexOf("copy_worker_assets()", StringComparison.Ordinal);
        Assert.IsTrue(copyFunctionIndex >= 0, "deploy.sh must define copy_worker_assets.");

        var copyFunction = script[copyFunctionIndex..];
        var nextFunctionIndex = copyFunction.IndexOf("\ncleanup_worker_shard_assets()", StringComparison.Ordinal);
        if (nextFunctionIndex >= 0)
            copyFunction = copyFunction[..nextFunctionIndex];

        Assert.IsTrue(
            copyFunction.Contains("shards/modules", StringComparison.Ordinal),
            "copy_worker_assets must stage binary module indexes as Worker assets.");
        Assert.IsTrue(
            copyFunction.Contains("*.index.bin", StringComparison.Ordinal),
            "copy_worker_assets must copy only binary index assets for modules.");
    }

    [TestMethod]
    public void Deploy_ShouldGenerateEmbeddedWorkerRuntimeInfo()
    {
        var script = File.ReadAllText(FindDeployScript());

        Assert.IsTrue(
            script.Contains("generate_worker_runtime_info", StringComparison.Ordinal),
            "deploy.sh must generate embedded Worker runtime info from info.json.");
        Assert.IsTrue(
            script.Contains("generated-runtime-info.ts", StringComparison.Ordinal),
            "deploy.sh must write the generated runtime info module consumed by the Worker.");
        Assert.IsTrue(
            script.Contains("generate_worker_runtime_info \"$DATASET_KEY\" \"$RELEASE_ID\"", StringComparison.Ordinal),
            "deploy.sh must generate runtime info after staging Worker assets and before Worker tests/deploy.");
    }

    [TestMethod]
    public void ValidateEndpoint_ShouldAcceptModuleOnlyRelease()
    {
        var script = File.ReadAllText(FindDeployScript());
        var validateFunctionIndex = script.IndexOf("validate_endpoint()", StringComparison.Ordinal);
        Assert.IsTrue(validateFunctionIndex >= 0, "deploy.sh must define validate_endpoint.");

        var validateFunction = script[validateFunctionIndex..];
        var nextFunctionIndex = validateFunction.IndexOf("\ndeploy_worker()", StringComparison.Ordinal);
        if (nextFunctionIndex >= 0)
            validateFunction = validateFunction[..nextFunctionIndex];

        Assert.IsTrue(
            validateFunction.Contains("module_shards", StringComparison.Ordinal),
            "validate_endpoint must accept releases published only under module_shards.");
        Assert.IsTrue(
            validateFunction.Contains("releaseMatches", StringComparison.Ordinal),
            "validate_endpoint must validate the requested release against base and module release references.");
    }

    [TestMethod]
    public void FetchJson_ShouldRetryTransientEndpointFailures()
    {
        var script = File.ReadAllText(FindDeployScript());
        var fetchFunctionIndex = script.IndexOf("fetch_json()", StringComparison.Ordinal);
        Assert.IsTrue(fetchFunctionIndex >= 0, "deploy.sh must define fetch_json.");

        var fetchFunction = script[fetchFunctionIndex..];
        var nextFunctionIndex = fetchFunction.IndexOf("\nmask_cnpj_for_path()", StringComparison.Ordinal);
        if (nextFunctionIndex >= 0)
            fetchFunction = fetchFunction[..nextFunctionIndex];

        Assert.IsTrue(
            fetchFunction.Contains("OPENCNPJ_FETCH_JSON_RETRIES", StringComparison.Ordinal),
            "fetch_json must allow retry count configuration for transient post-deploy endpoint failures.");
        Assert.IsTrue(
            fetchFunction.Contains("sleep \"$retry_delay_seconds\"", StringComparison.Ordinal),
            "fetch_json must sleep between retries instead of failing immediately.");
        Assert.IsTrue(
            fetchFunction.Contains("curl -fsS \"$url\"", StringComparison.Ordinal),
            "fetch_json must keep using curl with failure status handling.");
    }

    [TestMethod]
    public void DeleteOldRelease_ShouldNotPurgeBaseReleaseStillReferencedByInfo()
    {
        var script = File.ReadAllText(FindDeployScript());

        Assert.IsTrue(
            script.Contains("delete_old_releases \"$OLD_INFO\" \"$NEW_INFO\"", StringComparison.Ordinal),
            "deploy.sh must compare old and new info.json before deleting releases.");
        Assert.IsTrue(
            script.Contains("collectBaseReleases", StringComparison.Ordinal),
            "deploy.sh must keep base releases that are still referenced by the new info.json.");
        Assert.IsTrue(
            script.Contains("collectModuleReleases", StringComparison.Ordinal),
            "deploy.sh must also clean up stale module releases.");
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
