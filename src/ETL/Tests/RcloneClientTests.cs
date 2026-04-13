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
    public void BuildRemoteMd5SumArguments_DoesNotUse_FilesOnlyFlag()
    {
        var command = RcloneClient.BuildRemoteMd5SumArgumentsForTest(
            "Opencnpj:opencnpj/files/shards/releases/abc",
            ["*.ndjson"]);

        StringAssert.StartsWith(command, "md5sum ");
        Assert.IsFalse(command.Contains("--files-only", StringComparison.Ordinal));
        Assert.IsFalse(command.Contains("--recursive", StringComparison.Ordinal));
    }

    [TestMethod]
    public void NormalizeBufferSize_UsesSafeDefault_WhenMissing()
    {
        Assert.AreEqual("16M", RcloneClient.NormalizeBufferSizeForTest(null));
        Assert.AreEqual("16M", RcloneClient.NormalizeBufferSizeForTest(""));
    }

    [TestMethod]
    public void BuildFilesFromArgument_UsesRawMode()
    {
        var result = RcloneClient.BuildFilesFromArgumentForTest("/tmp/files.txt");

        Assert.AreEqual("--files-from-raw \"/tmp/files.txt\" ", result);
    }
}
