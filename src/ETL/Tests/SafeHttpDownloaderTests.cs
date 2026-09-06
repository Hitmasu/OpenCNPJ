using System.Net;
using System.Text;
using CNPJExporter.Integrations;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class SafeHttpDownloaderTests
{
    [TestMethod]
    public async Task DownloadAsync_ShouldPublishFile_WhenExpectedLengthMatches()
    {
        var data = Encoding.UTF8.GetBytes("safe-download");
        using var content = new ByteArrayContent(data);
        using var http = CreateHttpClient(content);
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");

        try
        {
            var result = await SafeHttpDownloader.DownloadAsync(
                http,
                new Uri("https://example.invalid/dataset.zip"),
                destinationPath,
                new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                data.Length);

            Assert.AreEqual(data.Length, result.BytesReceived);
            Assert.AreEqual(data.Length, result.ExpectedContentLength);
            CollectionAssert.AreEqual(data, await File.ReadAllBytesAsync(destinationPath));
            Assert.IsFalse(File.Exists(destinationPath + ".part"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldRejectTruncatedDownload_AndPreserveExistingFile()
    {
        var previousData = Encoding.UTF8.GetBytes("previous-valid-file");
        var downloadedData = Encoding.UTF8.GetBytes("short");
        const long expectedLength = 10;
        using var content = new ByteArrayContent(downloadedData);
        content.Headers.ContentLength = expectedLength;
        using var http = CreateHttpClient(content);
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");
        await File.WriteAllBytesAsync(destinationPath, previousData);

        try
        {
            await Assert.ThrowsExceptionAsync<InvalidDataException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                    expectedLength));

            CollectionAssert.AreEqual(previousData, await File.ReadAllBytesAsync(destinationPath));
            Assert.IsFalse(File.Exists(destinationPath + ".part"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldAbortStreaming_WhenUnknownLengthExceedsLimit()
    {
        var data = Encoding.UTF8.GetBytes("too-large");
        using var stream = new NonSeekableMemoryStream(data);
        using var content = new StreamContent(stream);
        using var http = CreateHttpClient(content);
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");

        try
        {
            await Assert.ThrowsExceptionAsync<InvalidDataException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 4)));

            Assert.IsFalse(File.Exists(destinationPath));
            Assert.IsFalse(File.Exists(destinationPath + ".part"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldRejectDeclaredLengthAboveLimit_BeforeWritingFile()
    {
        using var content = new ByteArrayContent(new byte[16]);
        using var http = CreateHttpClient(content);
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");

        try
        {
            await Assert.ThrowsExceptionAsync<InvalidDataException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 8)));

            Assert.IsFalse(File.Exists(destinationPath));
            Assert.IsFalse(File.Exists(destinationPath + ".part"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldApplyDeadline_ToHttpRequest()
    {
        using var http = new HttpClient(new DelayedHandler());
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");

        try
        {
            await Assert.ThrowsExceptionAsync<TimeoutException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromMilliseconds(50), 1024)));

            Assert.IsFalse(File.Exists(destinationPath));
            Assert.IsFalse(File.Exists(destinationPath + ".part"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static HttpClient CreateHttpClient(HttpContent content) =>
        new(new StaticResponseHandler(content));

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), $"opencnpj-safe-download-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class StaticResponseHandler(HttpContent content) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                RequestMessage = request,
                Content = content
            });
    }

    private sealed class DelayedHandler : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK) { RequestMessage = request };
        }
    }

    private sealed class NonSeekableMemoryStream(byte[] data) : MemoryStream(data)
    {
        public override bool CanSeek => false;

        public override long Seek(long offset, SeekOrigin loc) =>
            throw new NotSupportedException();

        public override long Position
        {
            get => base.Position;
            set => throw new NotSupportedException();
        }
    }
}
