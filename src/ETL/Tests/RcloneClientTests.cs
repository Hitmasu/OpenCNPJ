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
    public void BuildRemoteMd5SumArguments_KeepsBroadFilterValidation()
    {
        var command = RcloneClient.BuildRemoteMd5SumArgumentsForTest(
            "Opencnpj:opencnpj/files/shards/releases/abc",
            ["*.ndjson", "*.index.bin"]);

        Assert.AreEqual(
            "md5sum \"Opencnpj:opencnpj/files/shards/releases/abc\" --filter \"+ *.ndjson\" --filter \"+ *.index.bin\" --filter \"- **\" ",
            command);
    }

    [TestMethod]
    public void BuildSelectedRemoteMd5SumArguments_UsesFilesFromRaw()
    {
        var command = RcloneClient.BuildSelectedRemoteMd5SumArgumentsForTest(
            "Opencnpj:opencnpj/files/shards/releases/abc",
            "/tmp/selected files.txt");

        Assert.AreEqual(
            "md5sum \"Opencnpj:opencnpj/files/shards/releases/abc\" --files-from-raw \"/tmp/selected files.txt\" ",
            command);
    }

    [TestMethod]
    public void BuildSelectedRemoteMd5SumArguments_DoesNotUseBroadExtensionFilters()
    {
        var command = RcloneClient.BuildSelectedRemoteMd5SumArgumentsForTest(
            "Opencnpj:opencnpj/files/shards/releases/abc",
            "/tmp/selected.txt");

        Assert.IsFalse(command.Contains("--filter", StringComparison.Ordinal));
        Assert.IsFalse(command.Contains("*.ndjson", StringComparison.Ordinal));
        Assert.IsFalse(command.Contains("unselected.ndjson", StringComparison.Ordinal));
    }

    [TestMethod]
    public void BuildSelectedUploadFiles_PreservesRelativePaths()
    {
        var files = RcloneClient.BuildSelectedUploadFilesForTest(
            [
                "z file.ndjson",
                "sub dir/a.index.bin",
                "routing/607.routing.bin"
            ]);

        CollectionAssert.AreEqual(
            new[]
            {
                "routing/607.routing.bin",
                "sub dir/a.index.bin",
                "z file.ndjson"
            },
            files.ToArray());
    }

    [TestMethod]
    public void BuildSelectedUploadFiles_ReturnsEmpty_WhenNoFilesWereSelected()
    {
        var files = RcloneClient.BuildSelectedUploadFilesForTest([]);

        Assert.AreEqual(0, files.Count);
    }

    [TestMethod]
    public async Task UploadSelectedFilesAsync_ReturnsTrue_WhenNoFilesWereSelected()
    {
        var uploaded = await RcloneClient.UploadSelectedFilesAsync(
            "unused-local-folder",
            "unused-remote-folder",
            []);

        Assert.IsTrue(uploaded);
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

    [TestMethod]
    public void ResolveExecutable_UsesDefault_WhenMissing()
    {
        Assert.AreEqual("rclone", RcloneClient.ResolveExecutableForTest(null));
        Assert.AreEqual("rclone", RcloneClient.ResolveExecutableForTest(""));
        Assert.AreEqual("rclone", RcloneClient.ResolveExecutableForTest("   "));
    }

    [TestMethod]
    public void ResolveExecutable_TrimsExplicitCommand()
    {
        Assert.AreEqual("custom-rclone", RcloneClient.ResolveExecutableForTest(" custom-rclone "));
    }

    [TestMethod]
    public void ResolveExecutable_RetainsAbsolutePath()
    {
        const string executable = "/opt/rclone/rclone";

        Assert.AreEqual(executable, RcloneClient.ResolveExecutableForTest(executable));
    }
}
