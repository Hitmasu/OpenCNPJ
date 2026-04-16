using CNPJExporter.Processors;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ShardReleasePlanTests
{
    [TestMethod]
    public void BuildReleasePlan_WithPartialPrefixes_ShouldGenerateOnlyRequestedPrefixes()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"opencnpj-release-plan-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var plan = ParquetIngestor.BuildReleasePlanForTest(
                tempDir,
                ["000", "001", "002"],
                ["001"]);

            CollectionAssert.AreEqual(Array.Empty<string>(), plan.UploadOnly.ToArray());
            CollectionAssert.AreEqual(new[] { "001" }, plan.ToGenerate.ToArray());
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [TestMethod]
    public async Task CountShardRecordsFromIndexDirectory_ShouldSumBinaryIndexHeaders()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"opencnpj-index-count-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        try
        {
            await WriteShardAsync(tempDir, "000", ["00000000000191", "00000000000272"]);
            await WriteShardAsync(tempDir, "001", ["00100000000101"]);
            await File.WriteAllBytesAsync(Path.Combine(tempDir, "broken.index.bin"), [1, 2, 3]);

            var total = ParquetIngestor.CountShardRecordsFromIndexDirectoryForTest(tempDir);

            Assert.AreEqual(3, total);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [TestMethod]
    public void InfoJsonEncoding_ShouldNotEmitUtf8Bom()
    {
        Assert.AreEqual(0, ParquetIngestor.InfoJsonEncodingForTest.GetPreamble().Length);
    }

    private static async Task WriteShardAsync(string directory, string prefix, IReadOnlyList<string> cnpjs)
    {
        using var writer = new BinaryIndexedShardWriter(
            Path.Combine(directory, $"{prefix}.ndjson"),
            Path.Combine(directory, $"{prefix}.index.bin"));

        foreach (var cnpj in cnpjs)
            await writer.AppendAsync(cnpj, "{\"cnpj\":\"" + cnpj + "\",\"cno\":null}");

        await writer.FlushAsync();
    }
}
