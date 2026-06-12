using CNPJExporter.Configuration;
using CNPJExporter.Integrations;
using CNPJExporter.Processors.BigQuery;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class BigQueryPublicationPlannerTests
{
    [TestMethod]
    public void Build_ShouldMapCanonicalParquetSources()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-bigquery-plan-{Guid.NewGuid():N}");
        try
        {
            var sourcePath = Path.Combine(tempRoot, "receita.parquet");
            Touch(sourcePath);

            var plan = BigQueryPublicationPlanner.Build(
                new AppConfig.BigQuerySettings
                {
                    Enabled = true,
                    ProjectId = "opencnpj-bigquery",
                    Dataset = "public",
                    TablePrefix = "oc_"
                },
                "release-1",
                [new BigQueryParquetSource("receita", [sourcePath])]);

            Assert.AreEqual("oc_receita", plan.Tables.Single().DestinationTableName);
            Assert.AreEqual(sourcePath, plan.Tables.Single().SourcePaths.Single());
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public void Build_ShouldRejectDisabledBigQuerySettings()
    {
        var ex = Assert.ThrowsException<InvalidOperationException>(() =>
            BigQueryPublicationPlanner.Build(
                new AppConfig.BigQuerySettings
                {
                    Enabled = false
                },
                "release-1",
                []));

        StringAssert.Contains(ex.Message, "BigQuery.Enabled=false");
    }

    [TestMethod]
    public void Build_ShouldThrowClearError_WhenEnabledConfigIsIncomplete()
    {
        var ex = Assert.ThrowsException<InvalidOperationException>(() =>
            BigQueryPublicationPlanner.Build(
                new AppConfig.BigQuerySettings
                {
                    Enabled = true,
                    Dataset = "public"
                },
                "release-1",
                []));

        StringAssert.Contains(ex.Message, "BigQuery.ProjectId");
    }

    [TestMethod]
    public void Planner_ShouldNotKnowModuleSpecificTables()
    {
        var plannerSource = File.ReadAllText(FindPlannerSource());

        Assert.IsFalse(plannerSource.Contains("empresa", StringComparison.OrdinalIgnoreCase));
        Assert.IsFalse(plannerSource.Contains("estabelecimento", StringComparison.OrdinalIgnoreCase));
        Assert.IsFalse(plannerSource.Contains("cno", StringComparison.OrdinalIgnoreCase));
        Assert.IsFalse(plannerSource.Contains("rntrc", StringComparison.OrdinalIgnoreCase));
    }

    private static string FindPlannerSource()
    {
        var current = AppContext.BaseDirectory;
        while (!string.IsNullOrWhiteSpace(current))
        {
            var candidate = Path.Combine(current, "src", "ETL", "Processor", "BigQuery", "BigQueryPublicationPlanner.cs");
            if (File.Exists(candidate))
                return candidate;

            current = Directory.GetParent(current)?.FullName;
        }

        throw new FileNotFoundException("BigQueryPublicationPlanner.cs não encontrado.");
    }

    private static void Touch(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, "placeholder");
    }
}
