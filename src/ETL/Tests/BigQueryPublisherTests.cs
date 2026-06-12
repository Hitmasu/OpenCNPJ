using CNPJExporter.Processors.BigQuery;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class BigQueryPublisherTests
{
    [TestMethod]
    public async Task PublishAsync_ShouldLoadStagingBeforeReplacingFinalTable()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-bigquery-publish-{Guid.NewGuid():N}");
        try
        {
            var sourcePath = Path.Combine(tempRoot, "receita.parquet");
            Directory.CreateDirectory(tempRoot);
            File.WriteAllText(sourcePath, "placeholder");

            var runner = new RecordingBigQueryCommandRunner();
            var publisher = new BigQueryPublisher(runner);
            var plan = new BigQueryPublicationPlan(
                ProjectId: "opencnpj-bigquery",
                Dataset: "public",
                ReleaseId: "release-1",
                BqExecutable: "bq",
                Location: "US",
                KeepStagingTables: false,
                Tables: [new BigQueryTablePublication("receita", "receita", [sourcePath])]);

            await publisher.PublishAsync(plan);

            CollectionAssert.AreEqual(
                new[] { "show", "load", "cp", "rm" },
                runner.Commands.Select(CommandName).ToArray());

            var showCommand = string.Join(" ", runner.Commands[0].Args);
            StringAssert.Contains(showCommand, "opencnpj-bigquery:public");
            Assert.IsFalse(showCommand.Contains(".receita", StringComparison.Ordinal));

            var loadCommand = string.Join(" ", runner.Commands[1].Args);
            StringAssert.Contains(loadCommand, "--location=US");
            StringAssert.Contains(loadCommand, "opencnpj-bigquery:public.receita__staging_release_1");
            StringAssert.Contains(loadCommand, sourcePath);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task PublishAsync_ShouldAppendAdditionalParquetPartsToSameStagingTable()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-bigquery-publish-{Guid.NewGuid():N}");
        try
        {
            var sourcePaths = new[]
            {
                Path.Combine(tempRoot, "part-00000.parquet"),
                Path.Combine(tempRoot, "part-00001.parquet")
            };
            Directory.CreateDirectory(tempRoot);
            foreach (var sourcePath in sourcePaths)
                File.WriteAllText(sourcePath, "placeholder");

            var runner = new RecordingBigQueryCommandRunner();
            var publisher = new BigQueryPublisher(runner);
            var plan = new BigQueryPublicationPlan(
                ProjectId: "opencnpj-bigquery",
                Dataset: "public",
                ReleaseId: "release-1",
                BqExecutable: "bq",
                Location: "US",
                KeepStagingTables: false,
                Tables: [new BigQueryTablePublication("receita", "receita", sourcePaths)]);

            await publisher.PublishAsync(plan);

            CollectionAssert.AreEqual(
                new[] { "show", "load", "load", "cp", "rm" },
                runner.Commands.Select(CommandName).ToArray());

            var firstLoad = string.Join(" ", runner.Commands[1].Args);
            var secondLoad = string.Join(" ", runner.Commands[2].Args);
            StringAssert.Contains(firstLoad, "--replace");
            Assert.IsFalse(secondLoad.Contains("--replace", StringComparison.Ordinal));
            StringAssert.Contains(firstLoad, sourcePaths[0]);
            StringAssert.Contains(secondLoad, sourcePaths[1]);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static string CommandName(RecordedBigQueryCommand command) =>
        command.Args.First(arg => arg is "show" or "load" or "cp" or "rm");

    private sealed record RecordedBigQueryCommand(string Executable, IReadOnlyList<string> Args);

    private sealed class RecordingBigQueryCommandRunner : IBigQueryCommandRunner
    {
        public List<RecordedBigQueryCommand> Commands { get; } = [];

        public Task RunAsync(string executable, IReadOnlyList<string> arguments, CancellationToken cancellationToken = default)
        {
            Commands.Add(new RecordedBigQueryCommand(executable, arguments.ToArray()));
            return Task.CompletedTask;
        }
    }
}
