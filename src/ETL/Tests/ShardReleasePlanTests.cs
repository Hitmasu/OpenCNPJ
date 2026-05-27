using System.Buffers.Binary;
using System.Text;
using CNPJExporter.Processors;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ShardReleasePlanTests
{
    [TestMethod]
    public void BuildReleasePlan_WithMissingLocalShards_ShouldGenerateMissingAndUploadExisting()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"opencnpj-release-plan-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var plan = ParquetIngestor.BuildReleasePlanForTest(
                tempDir,
                ["000", "001", "002"]);

            CollectionAssert.AreEqual(Array.Empty<string>(), plan.UploadOnly.ToArray());
            CollectionAssert.AreEqual(new[] { "000", "001", "002" }, plan.ToGenerate.ToArray());
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

    [TestMethod]
    public async Task BinaryIndexedShardWriter_ShouldSortIndexEntriesByCnpj()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"opencnpj-index-sort-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        var dataPath = Path.Combine(tempDir, "607.ndjson");
        var indexPath = Path.Combine(tempDir, "607.index.bin");

        try
        {
            using (var writer = new BinaryIndexedShardWriter(dataPath, indexPath))
            {
                await writer.AppendAsync("60700000000002", """{"cnpj":"60700000000002"}""");
                await writer.AppendAsync("60700000000001", """{"cnpj":"60700000000001","nome":"Sócia"}""");
                await writer.FlushAsync();
            }

            var indexBytes = await File.ReadAllBytesAsync(indexPath);
            Assert.AreEqual(2u, BinaryPrimitives.ReadUInt32LittleEndian(indexBytes.AsSpan(4, sizeof(uint))));

            var firstCnpj = Encoding.ASCII.GetString(indexBytes, 8, 14);
            var secondCnpj = Encoding.ASCII.GetString(indexBytes, 8 + 26, 14);
            Assert.AreEqual("60700000000001", firstCnpj);
            Assert.AreEqual("60700000000002", secondCnpj);

            var firstOffset = BinaryPrimitives.ReadUInt64LittleEndian(indexBytes.AsSpan(8 + 14, sizeof(ulong)));
            var firstLength = BinaryPrimitives.ReadUInt32LittleEndian(indexBytes.AsSpan(8 + 14 + sizeof(ulong), sizeof(uint)));
            var dataBytes = await File.ReadAllBytesAsync(dataPath);
            var firstPayload = Encoding.UTF8.GetString(dataBytes.AsSpan((int)firstOffset, (int)firstLength));
            StringAssert.Contains(firstPayload, "60700000000001");
            StringAssert.Contains(firstPayload, "Sócia");
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [TestMethod]
    public void BuildInitialShardRangesForTest_ShouldPreSplitNumericPrefix()
    {
        var ranges = ParquetIngestor.BuildInitialShardRangesForTest("607", 5)
            .Select(range => range.ToString())
            .ToArray();

        CollectionAssert.AreEqual(
            new[]
            {
                "[60700000, 60720000)",
                "[60720000, 60740000)",
                "[60740000, 60760000)",
                "[60760000, 60780000)",
                "[60780000, 60800000)"
            },
            ranges);
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
