using CNPJExporter.Processors;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class SegmentRoutingIndexBuilderTests
{
    [TestMethod]
    public async Task Builder_ShouldMergeAndReplaceSegmentReferences()
    {
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-routing-{Guid.NewGuid():N}");

        try
        {
            var oldSegment = Path.Combine(tempRoot, "old");
            var currentSegment = Path.Combine(tempRoot, "current");
            Directory.CreateDirectory(oldSegment);
            Directory.CreateDirectory(currentSegment);

            await WriteShardAsync(
                oldSegment,
                "12A",
                ("12ABC34501DE35", """{"cnpj":"12ABC34501DE35","items":[1]}"""));
            await WriteShardAsync(
                currentSegment,
                "12A",
                ("12ABC34501DE35", """{"cnpj":"12ABC34501DE35","items":[2]}"""),
                ("12XYZ34501DE35", """{"cnpj":"12XYZ34501DE35","items":[3]}"""));

            var first = new SegmentRoutingIndexBuilder();
            first.AddSegment("2017", oldSegment);
            var routingV1 = Path.Combine(tempRoot, "routing-v1");
            first.WriteDirectory(routingV1);

            var second = new SegmentRoutingIndexBuilder();
            second.LoadDirectory(routingV1);
            second.AddSegment("2026-03", currentSegment);
            var routingV2 = Path.Combine(tempRoot, "routing-v2");
            second.WriteDirectory(routingV2);

            var loaded = new SegmentRoutingIndexBuilder();
            loaded.LoadDirectory(routingV2);
            CollectionAssert.AreEqual(
                new[] { "2017", "2026-03" },
                loaded
                    .GetReferencesForTest("12A", "12ABC34501DE35")
                    .Select(reference => reference.SegmentId)
                    .ToArray());
            CollectionAssert.AreEqual(
                new[] { "2026-03" },
                loaded
                    .GetReferencesForTest("12A", "12XYZ34501DE35")
                    .Select(reference => reference.SegmentId)
                    .ToArray());

            second.RemoveSegment("2017");
            second.AddSegment("2017", currentSegment);
            var replaced = second.GetReferencesForTest(
                "12A",
                "12ABC34501DE35");
            Assert.AreEqual(2, replaced.Count);
            Assert.AreEqual(
                replaced.Single(reference =>
                    reference.SegmentId == "2026-03").Length,
                replaced.Single(reference =>
                    reference.SegmentId == "2017").Length);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task Builder_ShouldCoalesceAdjacentChunksForSameCnpjAndSegment()
    {
        var tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"opencnpj-routing-chunks-{Guid.NewGuid():N}");
        try
        {
            Directory.CreateDirectory(tempRoot);
            await WriteShardAsync(
                tempRoot,
                "12A",
                ("12ABC34501DE35", """{"cnpj":"12ABC34501DE35","items":[1]}"""),
                ("12ABC34501DE35", """{"cnpj":"12ABC34501DE35","items":[2]}"""));

            var builder = new SegmentRoutingIndexBuilder();
            builder.AddSegment("2026-03", tempRoot);

            var references = builder.GetReferencesForTest(
                "12A",
                "12ABC34501DE35");
            Assert.AreEqual(1, references.Count);
            var reference = references[0];
            Assert.AreEqual("2026-03", reference.SegmentId);
            Assert.AreEqual(0UL, reference.Offset);
            Assert.AreEqual(
                checked((uint)new FileInfo(
                    Path.Combine(tempRoot, "12A.ndjson")).Length),
                reference.Length);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static async Task WriteShardAsync(
        string directory,
        string prefix,
        params (string Cnpj, string Json)[] records)
    {
        using var writer = new BinaryIndexedShardWriter(
            Path.Combine(directory, $"{prefix}.ndjson"),
            Path.Combine(directory, $"{prefix}.index.bin"));
        foreach (var (cnpj, json) in records)
            await writer.AppendAsync(cnpj, json);
        await writer.FlushAsync();
    }
}
