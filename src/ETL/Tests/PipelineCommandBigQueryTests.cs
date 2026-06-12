using CNPJExporter.Commands;
using CNPJExporter.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class PipelineCommandBigQueryTests
{
    [TestMethod]
    public void ShouldRunBigQuery_ShouldWarnAndSkip_WhenConfigIsDisabled()
    {
        var warnings = new List<string>();

        var shouldRun = PipelineCommand.ShouldRunBigQuery(
            new AppConfig.BigQuerySettings { Enabled = false },
            warnings.Add);

        Assert.IsFalse(shouldRun);
        Assert.AreEqual(1, warnings.Count);
        StringAssert.Contains(warnings.Single(), "BigQuery não habilitado");
    }

    [TestMethod]
    public void ShouldRunBigQuery_ShouldNotWarn_WhenConfigIsEnabled()
    {
        var warnings = new List<string>();

        var shouldRun = PipelineCommand.ShouldRunBigQuery(
            new AppConfig.BigQuerySettings { Enabled = true },
            warnings.Add);

        Assert.IsTrue(shouldRun);
        Assert.AreEqual(0, warnings.Count);
    }
}
