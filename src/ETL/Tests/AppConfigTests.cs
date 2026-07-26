using CNPJExporter.Configuration;
using CnoIntegrationOptions = CNPJExporter.Modules.Cno.Configuration.IntegrationOptions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RntrcIntegrationOptions = CNPJExporter.Modules.Rntrc.Configuration.IntegrationOptions;
using PortalTransparenciaIntegrationOptions = CNPJExporter.Modules.PortalTransparencia.Configuration.IntegrationOptions;

namespace ETL.Tests;

[TestClass]
public sealed class AppConfigTests
{
    [TestMethod]
    public void ModuleSourceUrls_ShouldNotBeHardCodedOutsideConfigJson()
    {
        Assert.AreEqual(string.Empty, new AppConfig.CnoIntegrationSettings().PublicShareRoot);
        Assert.AreEqual(string.Empty, new AppConfig.RntrcIntegrationSettings().PackageShowUrl);
        Assert.AreEqual(string.Empty, new AppConfig.PortalTransparenciaIntegrationSettings().CatalogBaseUrl);
        Assert.AreEqual(string.Empty, new CnoIntegrationOptions().PublicShareRoot);
        Assert.AreEqual(string.Empty, new RntrcIntegrationOptions().PackageShowUrl);
        Assert.AreEqual(string.Empty, new PortalTransparenciaIntegrationOptions().CatalogBaseUrl);
    }

    [TestMethod]
    public void PortalTransparencia_ShouldUseBoundedDuckDbLimits()
    {
        var settings = new AppConfig.PortalTransparenciaIntegrationSettings();

        Assert.AreEqual("512MB", settings.DuckDbMemoryLimit);
        Assert.AreEqual("20GB", settings.DuckDbMaxTempDirectorySize);
    }

    [TestMethod]
    public void BigQuery_ShouldBeDisabledByDefault()
    {
        var settings = new AppConfig.BigQuerySettings();

        Assert.IsFalse(settings.Enabled);
        Assert.AreEqual(string.Empty, settings.ProjectId);
        Assert.AreEqual("bq", settings.BqExecutable);
    }

    [TestMethod]
    public void Load_ShouldOverrideBigQueryProjectIdFromEnvironment()
    {
        var previousEnabled = Environment.GetEnvironmentVariable(AppConfig.BigQueryEnabledEnvironmentVariable);
        var previous = Environment.GetEnvironmentVariable(AppConfig.BigQueryProjectIdEnvironmentVariable);
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-config-{Guid.NewGuid():N}");
        var configPath = Path.Combine(tempRoot, "config.json");

        try
        {
            Directory.CreateDirectory(tempRoot);
            File.WriteAllText(
                configPath,
                """
                {
                  "BigQuery": {
                    "Enabled": true,
                    "ProjectId": "project-from-config",
                    "Dataset": "public"
                  }
                }
                """);
            Environment.SetEnvironmentVariable(AppConfig.BigQueryEnabledEnvironmentVariable, "false");
            Environment.SetEnvironmentVariable(AppConfig.BigQueryProjectIdEnvironmentVariable, " project-from-env ");

            var config = AppConfig.Load(configPath);

            Assert.IsFalse(config.BigQuery.Enabled);
            Assert.AreEqual("project-from-env", config.BigQuery.ProjectId);
        }
        finally
        {
            Environment.SetEnvironmentVariable(AppConfig.BigQueryEnabledEnvironmentVariable, previousEnabled);
            Environment.SetEnvironmentVariable(AppConfig.BigQueryProjectIdEnvironmentVariable, previous);
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public void Load_ShouldEnableBigQueryFromEnvironment()
    {
        var previous = Environment.GetEnvironmentVariable(AppConfig.BigQueryEnabledEnvironmentVariable);
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-config-{Guid.NewGuid():N}");
        var configPath = Path.Combine(tempRoot, "config.json");

        try
        {
            Directory.CreateDirectory(tempRoot);
            File.WriteAllText(
                configPath,
                """
                {
                  "BigQuery": {
                    "Enabled": false,
                    "Dataset": "public"
                  }
                }
                """);
            Environment.SetEnvironmentVariable(AppConfig.BigQueryEnabledEnvironmentVariable, "true");

            var config = AppConfig.Load(configPath);

            Assert.IsTrue(config.BigQuery.Enabled);
        }
        finally
        {
            Environment.SetEnvironmentVariable(AppConfig.BigQueryEnabledEnvironmentVariable, previous);
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public void Load_ShouldRejectNonBooleanBigQueryEnabledEnvironmentValue()
    {
        var previous = Environment.GetEnvironmentVariable(AppConfig.BigQueryEnabledEnvironmentVariable);

        try
        {
            Environment.SetEnvironmentVariable(AppConfig.BigQueryEnabledEnvironmentVariable, "1");

            var ex = Assert.ThrowsException<InvalidOperationException>(() => AppConfig.Load("missing-config.json"));

            StringAssert.Contains(ex.Message, "true ou false");
        }
        finally
        {
            Environment.SetEnvironmentVariable(AppConfig.BigQueryEnabledEnvironmentVariable, previous);
        }
    }
}
