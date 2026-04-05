using CNPJExporter.Exporters;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public class RcloneClientTests
{
    [TestMethod]
    public void BuildFilterArguments_UsesOnlyFilterRules()
    {
        var result = RcloneClient.BuildFilterArgumentsForTest(["*.ndjson"]);

        Assert.AreEqual("--filter \"+ *.ndjson\" --filter \"- **\" ", result);
    }

    [TestMethod]
    public void IsUploadComplete_ReturnsTrue_WhenRemoteMatchesLocal()
    {
        Assert.IsTrue(RcloneClient.IsUploadCompleteForTest(10, 10));
    }

    [TestMethod]
    public void IsUploadComplete_ReturnsFalse_WhenRemoteIsMissingFiles()
    {
        Assert.IsFalse(RcloneClient.IsUploadCompleteForTest(10, 9));
    }

    [TestMethod]
    public void BuildUploadPlan_ReturnsOnlyMissingOrChangedFiles()
    {
        var local = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["001.ndjson"] = "aaa",
            ["002.ndjson"] = "bbb",
            ["003.ndjson"] = "ccc"
        };
        var remote = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["001.ndjson"] = "aaa",
            ["002.ndjson"] = "zzz"
        };

        var plan = RcloneClient.BuildUploadPlanForTest(local, remote);

        CollectionAssert.AreEqual(
            new[] { "002.ndjson", "003.ndjson" },
            plan.ToArray());
    }

    [TestMethod]
    public void BuildRemoteMd5SumArguments_DoesNotUse_FilesOnlyFlag()
    {
        var command = RcloneClient.BuildRemoteMd5SumArgumentsForTest(
            "Opencnpj:opencnpj/files/shards/releases/abc",
            ["*.ndjson"]);

        StringAssert.StartsWith(command, "md5sum ");
        Assert.IsFalse(command.Contains("--files-only", StringComparison.Ordinal));
        Assert.IsFalse(command.Contains("--recursive", StringComparison.Ordinal));
    }
}
