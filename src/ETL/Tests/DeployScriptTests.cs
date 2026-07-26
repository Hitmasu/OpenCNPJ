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
    public void Deploy_ShouldNotForceReceitaMonthByDefault()
    {
        var script = File.ReadAllText(FindDeployScript());

        StringAssert.Contains(
            script,
            "MONTH=\"\"");
        Assert.IsFalse(
            script.Contains("MONTH=\"2026-", StringComparison.Ordinal),
            "deploy.sh must not force a Receita month; --month should only be used when passed explicitly.");
        Assert.IsTrue(
            script.Contains("if [[ -n \"$MONTH\" ]]; then", StringComparison.Ordinal),
            "deploy.sh must keep --month conditional.");
    }

    [TestMethod]
    public void Deploy_ShouldRequireBigQueryCommandOnlyWhenBigQueryIsEnabled()
    {
        var script = File.ReadAllText(FindDeployScript());
        var functionIndex = script.IndexOf("require_bigquery_command_if_enabled()", StringComparison.Ordinal);
        Assert.IsTrue(functionIndex >= 0, "deploy.sh must define a conditional BigQuery command check.");

        var function = script[functionIndex..];
        var nextFunctionIndex = function.IndexOf("\njson_field()", StringComparison.Ordinal);
        if (nextFunctionIndex >= 0)
            function = function[..nextFunctionIndex];

        Assert.IsTrue(
            function.Contains("BigQuery.Enabled", StringComparison.Ordinal),
            "deploy.sh must be able to read the BigQuery enabled flag from ETL config.");
        Assert.IsTrue(
            script.Contains("OPENCNPJ_BIGQUERY_ENABLED", StringComparison.Ordinal),
            "deploy.sh must accept the BigQuery enabled flag from the deployment environment.");
        Assert.IsTrue(
            function.Contains("read_bigquery_enabled", StringComparison.Ordinal),
            "deploy.sh must resolve the BigQuery enabled flag through the override-aware helper.");
        Assert.IsTrue(
            function.Contains("BigQuery.BqExecutable", StringComparison.Ordinal),
            "deploy.sh must honor the configured bq executable.");
        Assert.IsTrue(
            function.Contains("BigQuery não habilitado", StringComparison.Ordinal),
            "deploy.sh must warn and continue when BigQuery is disabled.");
        Assert.IsTrue(
            function.Contains("BigQuery.ProjectId", StringComparison.Ordinal),
            "deploy.sh must validate ProjectId before running the ETL when BigQuery is enabled.");
        Assert.IsTrue(
            function.Contains("OPENCNPJ_BIGQUERY_PROJECT_ID", StringComparison.Ordinal),
            "deploy.sh must accept the BigQuery project id from the deployment environment.");
        Assert.IsTrue(
            function.Contains("BigQuery.Dataset", StringComparison.Ordinal),
            "deploy.sh must validate Dataset before running the ETL when BigQuery is enabled.");
        Assert.IsTrue(
            function.Contains("BigQuery.Location", StringComparison.Ordinal),
            "deploy.sh must honor the configured BigQuery location during validation.");
        Assert.IsTrue(
            function.Contains("require_command \"$executable\"", StringComparison.Ordinal),
            "deploy.sh must require bq only after BigQuery is enabled.");
        Assert.IsTrue(
            function.Contains("show --format=none \"${project_id}:${dataset}\"", StringComparison.Ordinal),
            "deploy.sh must validate ambient authentication and dataset access using the configured project.");
        Assert.IsFalse(
            function.Contains("GOOGLE_" + "APPLICATION_" + "CREDENTIALS", StringComparison.Ordinal),
            "deploy.sh must not require a key file.");
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
    public void ValidateEndpoint_ShouldAcceptModuleReleaseFromDatasets()
    {
        var script = File.ReadAllText(FindDeployScript());
        var validateFunctionIndex = script.IndexOf("validate_endpoint()", StringComparison.Ordinal);
        Assert.IsTrue(validateFunctionIndex >= 0, "deploy.sh must define validate_endpoint.");

        var validateFunction = script[validateFunctionIndex..];
        var nextFunctionIndex = validateFunction.IndexOf("\ndeploy_worker()", StringComparison.Ordinal);
        if (nextFunctionIndex >= 0)
            validateFunction = validateFunction[..nextFunctionIndex];

        Assert.IsTrue(
            validateFunction.Contains("info?.datasets", StringComparison.Ordinal),
            "validate_endpoint must accept module releases from the canonical datasets map.");
        Assert.IsFalse(
            validateFunction.Contains("module_shards", StringComparison.Ordinal),
            "validate_endpoint must not depend on the removed module_shards map.");
        Assert.IsTrue(
            validateFunction.Contains("releaseMatches", StringComparison.Ordinal),
            "validate_endpoint must validate the requested release against base and module release references.");
        Assert.IsTrue(
            validateFunction.Contains("OPENCNPJ_VALIDATE_RETRIES", StringComparison.Ordinal),
            "validate_endpoint must retry semantic validation while Worker/cache propagation settles.");
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
        Assert.IsTrue(
            script.Contains("collectRoutingReleases", StringComparison.Ordinal),
            "deploy.sh must compare routing releases from segmented modules.");
        Assert.IsTrue(
            script.Contains("collectSegmentReleases", StringComparison.Ordinal),
            "deploy.sh must preserve immutable segment releases still referenced by the new info.json.");
        Assert.IsTrue(
            script.Contains("/segments/${second}/${third}", StringComparison.Ordinal),
            "deploy.sh must delete an obsolete segment only at its exact module/segment/release path.");
    }

    [TestMethod]
    public void Deploy_ShouldPurgeCloudflareCacheAfterWorkerDeployBeforeValidation()
    {
        var script = File.ReadAllText(FindDeployScript());

        Assert.IsTrue(
            script.Contains("purge_cloudflare_cache()", StringComparison.Ordinal),
            "deploy.sh must define a Cloudflare cache purge step.");
        Assert.IsTrue(
            script.Contains("purge_everything", StringComparison.Ordinal),
            "deploy.sh must purge the zone cache because module changes can affect any cached CNPJ response.");

        var deployIndex = script.IndexOf("DEPLOY_URL=\"$(deploy_worker | tail -n 1)\"", StringComparison.Ordinal);
        var purgeIndex = script.IndexOf("purge_cloudflare_cache", deployIndex, StringComparison.Ordinal);
        var validateIndex = script.IndexOf("log \"Validando /info\"", StringComparison.Ordinal);

        Assert.IsTrue(deployIndex >= 0, "deploy.sh must deploy the Worker.");
        Assert.IsTrue(purgeIndex > deployIndex, "Cloudflare cache purge must run after Worker deploy.");
        Assert.IsTrue(validateIndex > purgeIndex, "Endpoint validation must run after cache purge.");
    }

    [TestMethod]
    public void ResolveDatasetKey_ShouldUseReleaseInfoAndIgnoreShardAssetDirectory()
    {
        var script = File.ReadAllText(FindDeployScript());
        var functionIndex = script.IndexOf("resolve_dataset_key()", StringComparison.Ordinal);
        Assert.IsTrue(functionIndex >= 0, "deploy.sh must define resolve_dataset_key.");

        var function = script[functionIndex..];
        var nextFunctionIndex = function.IndexOf("\nread_config_value()", StringComparison.Ordinal);
        if (nextFunctionIndex >= 0)
            function = function[..nextFunctionIndex];

        Assert.IsTrue(
            function.Contains("*/releases/${RELEASE_ID}/info.json", StringComparison.Ordinal),
            "resolve_dataset_key must resolve the dataset from the info.json generated for the current release.");
        Assert.IsTrue(
            function.Contains("^[0-9]{4}-[0-9]{2}$", StringComparison.Ordinal),
            "resolve_dataset_key must ignore non-dataset directories such as cnpj_shards/shards.");
    }

    [TestMethod]
    public void DockerEntrypoint_ShouldRunHourlyDeployWithoutForcingReceitaMonth()
    {
        var script = File.ReadAllText(FindDockerEntrypointScript());

        Assert.IsTrue(
            script.Contains("CHECK_INTERVAL_SECONDS=\"${OPENCNPJ_CHECK_INTERVAL_SECONDS:-3600}\"", StringComparison.Ordinal),
            "docker-entrypoint.sh must default to hourly execution.");
        Assert.IsTrue(
            script.Contains("while true; do", StringComparison.Ordinal),
            "docker-entrypoint.sh must keep the container alive between runs.");
        Assert.IsTrue(
            script.Contains("\"${DEPLOY_SCRIPT}\" --cleanup-on-success", StringComparison.Ordinal),
            "docker-entrypoint.sh must run deploy.sh through the normal production path.");
        Assert.IsFalse(
            script.Contains("--month", StringComparison.Ordinal),
            "docker-entrypoint.sh must not force a Receita month; otherwise every hourly run republishes the base dataset.");
        Assert.IsFalse(
            script.Contains("--release-id", StringComparison.Ordinal),
            "docker-entrypoint.sh should let deploy.sh generate a fresh release id only when there is work to publish.");
    }

    [TestMethod]
    public void Deploy_ShouldResumePendingOrIncompleteRelease()
    {
        var script = File.ReadAllText(FindDeployScript());

        StringAssert.Contains(script, "PENDING_RELEASE_FILE=");
        StringAssert.Contains(script, "read_pending_release_id");
        StringAssert.Contains(script, "discover_resumable_release_id");
        StringAssert.Contains(script, "PIPELINE_ARGS+=(--resume-existing-release)");
        StringAssert.Contains(script, "rm -f \"$PENDING_RELEASE_FILE\"");
    }

    [TestMethod]
    public void DockerEntrypoint_ShouldActivateBigQueryCredentialsFromBase64Environment()
    {
        var script = File.ReadAllText(FindDockerEntrypointScript());

        Assert.IsTrue(
            script.Contains("OPENCNPJ_GOOGLE_CREDENTIALS_BASE64", StringComparison.Ordinal),
            "docker-entrypoint.sh must accept BigQuery credentials from a Dokploy secret env.");
        Assert.IsTrue(
            script.Contains("OPENCNPJ_BIGQUERY_ENABLED", StringComparison.Ordinal),
            "docker-entrypoint.sh must activate BigQuery credentials when BigQuery is enabled by env.");
        Assert.IsTrue(
            script.Contains("base64 -d", StringComparison.Ordinal),
            "docker-entrypoint.sh must decode the credentials only at runtime.");
        Assert.IsTrue(
            script.Contains("gcloud auth activate-service-account", StringComparison.Ordinal),
            "docker-entrypoint.sh must activate the decoded credentials for bq.");
        Assert.IsTrue(
            script.Contains("rm -f \"$TMP_GOOGLE_CREDENTIALS\"", StringComparison.Ordinal),
            "docker-entrypoint.sh must remove the temporary credential file after activation.");
        Assert.IsFalse(
            script.Contains("GOOGLE_" + "APPLICATION_" + "CREDENTIALS", StringComparison.Ordinal),
            "docker-entrypoint.sh must not require a mounted credential file.");
    }

    [TestMethod]
    public void Dockerfile_ShouldUseEntrypointForDokployCommandOverrides()
    {
        var dockerfile = File.ReadAllText(FindRepoFile("Dockerfile"));

        Assert.IsTrue(
            dockerfile.Contains("ENTRYPOINT [\"/app/src/scripts/docker-entrypoint.sh\"]", StringComparison.Ordinal),
            "Dockerfile must use ENTRYPOINT so Dokploy command overrides cannot bypass the hourly loop.");
        Assert.IsFalse(
            dockerfile.Contains("CMD [\"/app/src/scripts/docker-entrypoint.sh\"]", StringComparison.Ordinal),
            "CMD is overridden by Dokploy command; the hourly loop must be the image ENTRYPOINT.");
        Assert.IsFalse(
            dockerfile.Contains("ENV CLOUDFLARE_ACCOUNT_ID", StringComparison.Ordinal),
            "Cloudflare account id must be provided by runtime configuration, not baked into the image.");
        Assert.IsFalse(
            dockerfile.Contains("ENV CLOUDFLARE_ZONE_ID", StringComparison.Ordinal),
            "Cloudflare zone id must be provided by runtime configuration, not baked into the image.");
        Assert.IsFalse(
            dockerfile.Contains("ENV CLOUDFLARE_API_TOKEN", StringComparison.Ordinal),
            "Cloudflare API token must be provided by runtime configuration, not baked into the image.");
    }

    private static string FindDeployScript()
    {
        return FindRepoFile("src", "scripts", "deploy.sh");
    }

    private static string FindDockerEntrypointScript()
    {
        return FindRepoFile("src", "scripts", "docker-entrypoint.sh");
    }

    private static string FindRepoFile(params string[] segments)
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null)
        {
            var candidate = Path.Combine([current.FullName, ..segments]);
            if (File.Exists(candidate))
                return candidate;

            current = current.Parent;
        }

        throw new FileNotFoundException($"Could not locate {Path.Combine(segments)}.");
    }
}
