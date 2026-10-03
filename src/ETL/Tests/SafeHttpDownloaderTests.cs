using System.Net;
using System.Net.Http.Headers;
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
            Assert.IsFalse(File.Exists(destinationPath + ".part.meta"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldPreservePartial_WhenDownloadEndsBeforeExpectedLength()
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
            await Assert.ThrowsExceptionAsync<IOException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                    expectedLength));

            CollectionAssert.AreEqual(previousData, await File.ReadAllBytesAsync(destinationPath));
            CollectionAssert.AreEqual(downloadedData, await File.ReadAllBytesAsync(destinationPath + ".part"));
            Assert.IsTrue(File.Exists(destinationPath + ".part.meta"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldRejectResponseLengthThatDiffersFromExpectedLength()
    {
        var data = Encoding.UTF8.GetBytes("content");
        using var content = new ByteArrayContent(data);
        content.Headers.ContentLength = data.Length + 1;
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
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                    data.Length));

            Assert.IsFalse(File.Exists(destinationPath));
            Assert.IsFalse(File.Exists(destinationPath + ".part"));
            Assert.IsFalse(File.Exists(destinationPath + ".part.meta"));
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
            Assert.IsFalse(File.Exists(destinationPath + ".part.meta"));
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
            Assert.IsFalse(File.Exists(destinationPath + ".part.meta"));
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
            Assert.IsFalse(File.Exists(destinationPath + ".part.meta"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldApplyDeadline_WhileStreaming()
    {
        using var stream = new SlowReadStream();
        using var content = new StreamContent(stream);
        using var http = CreateHttpClient(content);
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
            Assert.IsFalse(File.Exists(destinationPath + ".part.meta"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldPreservePartial_WhenStreamBecomesInactive()
    {
        var data = Encoding.UTF8.GetBytes("helloworld");
        var prefix = data[..5];
        using var stream = new PrefixThenStallStream(prefix);
        using var content = new StreamContent(stream);
        content.Headers.ContentLength = data.Length;
        using var http = CreateHttpClient(content);
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");

        try
        {
            var options = new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024)
            {
                InactivityTimeout = TimeSpan.FromMilliseconds(100)
            };

            await Assert.ThrowsExceptionAsync<TimeoutException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    options,
                    data.Length,
                    entityTag: "\"v1\""));

            CollectionAssert.AreEqual(prefix, await File.ReadAllBytesAsync(destinationPath + ".part"));
            Assert.IsTrue(File.Exists(destinationPath + ".part.meta"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldHonorCallerCancellation_AndPreserveExistingFile()
    {
        var previousData = Encoding.UTF8.GetBytes("previous-valid-file");
        using var stream = new SlowReadStream();
        using var content = new StreamContent(stream);
        using var http = CreateHttpClient(content);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");
        await File.WriteAllBytesAsync(destinationPath, previousData);

        try
        {
            try
            {
                await SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                    cancellationToken: cancellation.Token);
                Assert.Fail("O download deveria ter sido cancelado pelo chamador.");
            }
            catch (OperationCanceledException)
            {
                // Expected: caller cancellation must not be converted into a timeout.
            }

            CollectionAssert.AreEqual(previousData, await File.ReadAllBytesAsync(destinationPath));
            Assert.IsFalse(File.Exists(destinationPath + ".part"));
            Assert.IsFalse(File.Exists(destinationPath + ".part.meta"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldResumePartialDownload_WithRangeAndIfRange()
    {
        var data = Encoding.UTF8.GetBytes("helloworld");
        var prefix = data[..5];
        var suffix = data[5..];

        using var handler = new ScriptedHandler((attempt, _) => attempt switch
        {
            0 => CreateInterruptedResponse(prefix, data.Length, "\"v1\""),
            1 => CreatePartialResponse(suffix, prefix.Length, data.Length, "\"v1\""),
            _ => throw new InvalidOperationException("Nenhuma outra requisição era esperada.")
        });
        using var http = new HttpClient(handler);
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");

        try
        {
            await Assert.ThrowsExceptionAsync<IOException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                    data.Length,
                    entityTag: "\"v1\""));

            CollectionAssert.AreEqual(prefix, await File.ReadAllBytesAsync(destinationPath + ".part"));

            var result = await SafeHttpDownloader.DownloadAsync(
                http,
                new Uri("https://example.invalid/dataset.zip"),
                destinationPath,
                new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                data.Length,
                entityTag: "\"v1\"");

            Assert.AreEqual(2, handler.Requests.Count);
            Assert.IsNull(handler.Requests[0].Range);
            Assert.AreEqual("bytes=5-", handler.Requests[1].Range);
            Assert.AreEqual("\"v1\"", handler.Requests[1].IfRange);
            Assert.AreEqual(data.Length, result.BytesReceived);
            CollectionAssert.AreEqual(data, await File.ReadAllBytesAsync(destinationPath));
            Assert.IsFalse(File.Exists(destinationPath + ".part"));
            Assert.IsFalse(File.Exists(destinationPath + ".part.meta"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldRestartFromZero_WhenServerIgnoresRange()
    {
        var data = Encoding.UTF8.GetBytes("helloworld");
        var prefix = data[..5];

        using var handler = new ScriptedHandler((attempt, _) => attempt switch
        {
            0 => CreateInterruptedResponse(prefix, data.Length, "\"v1\""),
            1 => CreateFullResponse(data, "\"v1\""),
            _ => throw new InvalidOperationException("Nenhuma outra requisição era esperada.")
        });
        using var http = new HttpClient(handler);
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");

        try
        {
            await Assert.ThrowsExceptionAsync<IOException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                    data.Length,
                    entityTag: "\"v1\""));

            await SafeHttpDownloader.DownloadAsync(
                http,
                new Uri("https://example.invalid/dataset.zip"),
                destinationPath,
                new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                data.Length,
                entityTag: "\"v1\"");

            Assert.AreEqual("bytes=5-", handler.Requests[1].Range);
            CollectionAssert.AreEqual(data, await File.ReadAllBytesAsync(destinationPath));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldRestartFromZero_WhenEntityTagChanges()
    {
        var oldData = Encoding.UTF8.GetBytes("helloworld");
        var newData = Encoding.UTF8.GetBytes("HELLOWORLD");
        var prefix = oldData[..5];

        using var handler = new ScriptedHandler((attempt, _) => attempt switch
        {
            0 => CreateInterruptedResponse(prefix, oldData.Length, "\"v1\""),
            1 => CreateFullResponse(newData, "\"v2\""),
            _ => throw new InvalidOperationException("Nenhuma outra requisição era esperada.")
        });
        using var http = new HttpClient(handler);
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");

        try
        {
            await Assert.ThrowsExceptionAsync<IOException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                    oldData.Length,
                    entityTag: "\"v1\""));

            await SafeHttpDownloader.DownloadAsync(
                http,
                new Uri("https://example.invalid/dataset.zip"),
                destinationPath,
                new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                newData.Length,
                entityTag: "\"v2\"");

            Assert.AreEqual(2, handler.Requests.Count);
            Assert.IsNull(handler.Requests[1].Range);
            Assert.IsNull(handler.Requests[1].IfRange);
            CollectionAssert.AreEqual(newData, await File.ReadAllBytesAsync(destinationPath));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task DownloadAsync_ShouldPublishCompletePartial_WhenServerReturns416()
    {
        var data = Encoding.UTF8.GetBytes("helloworld");

        using var handler = new ScriptedHandler((attempt, _) => attempt switch
        {
            0 => CreateInterruptedResponse(data, data.Length, "\"v1\""),
            1 => CreateRangeNotSatisfiableResponse(data.Length),
            _ => throw new InvalidOperationException("Nenhuma outra requisição era esperada.")
        });
        using var http = new HttpClient(handler);
        var tempRoot = CreateTempDirectory();
        var destinationPath = Path.Combine(tempRoot, "dataset.zip");

        try
        {
            await Assert.ThrowsExceptionAsync<IOException>(() =>
                SafeHttpDownloader.DownloadAsync(
                    http,
                    new Uri("https://example.invalid/dataset.zip"),
                    destinationPath,
                    new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                    data.Length,
                    entityTag: "\"v1\""));

            Assert.AreEqual(data.Length, new FileInfo(destinationPath + ".part").Length);

            var result = await SafeHttpDownloader.DownloadAsync(
                http,
                new Uri("https://example.invalid/dataset.zip"),
                destinationPath,
                new DownloadSafetyOptions(TimeSpan.FromSeconds(5), 1024),
                data.Length,
                entityTag: "\"v1\"");

            Assert.AreEqual($"bytes={data.Length}-", handler.Requests[1].Range);
            Assert.AreEqual(data.Length, result.BytesReceived);
            CollectionAssert.AreEqual(data, await File.ReadAllBytesAsync(destinationPath));
            Assert.IsFalse(File.Exists(destinationPath + ".part"));
            Assert.IsFalse(File.Exists(destinationPath + ".part.meta"));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public void ForExpectedLength_ShouldUseExactKnownLength_AndFallbackForUnknownLength()
    {
        const long expectedLength = 1234;

        var known = DownloadSafetyDefaults.ForExpectedLength(expectedLength);
        var unknown = DownloadSafetyDefaults.ForExpectedLength(null);

        Assert.AreEqual(expectedLength, known.MaxBytes);
        Assert.AreEqual(DownloadSafetyDefaults.DownloadTimeout, known.Timeout);
        Assert.AreEqual(DownloadSafetyDefaults.StreamInactivityTimeout, known.InactivityTimeout);
        Assert.AreEqual(DownloadSafetyDefaults.UnknownContentLengthMaxBytes, unknown.MaxBytes);
        Assert.AreEqual(DownloadSafetyDefaults.DownloadTimeout, unknown.Timeout);
        Assert.AreEqual(DownloadSafetyDefaults.StreamInactivityTimeout, unknown.InactivityTimeout);
    }

    private static HttpClient CreateHttpClient(HttpContent content) =>
        new(new StaticResponseHandler(content));

    private static HttpResponseMessage CreateInterruptedResponse(
        byte[] prefix,
        int expectedLength,
        string entityTag)
    {
        var content = new StreamContent(new PrefixThenThrowStream(prefix));
        content.Headers.ContentLength = expectedLength;
        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = content
        };
        response.Headers.ETag = EntityTagHeaderValue.Parse(entityTag);
        return response;
    }

    private static HttpResponseMessage CreatePartialResponse(
        byte[] suffix,
        int offset,
        int totalLength,
        string entityTag)
    {
        var response = new HttpResponseMessage(HttpStatusCode.PartialContent)
        {
            Content = new ByteArrayContent(suffix)
        };
        response.Headers.ETag = EntityTagHeaderValue.Parse(entityTag);
        response.Content.Headers.ContentRange = ContentRangeHeaderValue.Parse(
            $"bytes {offset}-{totalLength - 1}/{totalLength}");
        return response;
    }

    private static HttpResponseMessage CreateFullResponse(byte[] data, string entityTag)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent(data)
        };
        response.Headers.ETag = EntityTagHeaderValue.Parse(entityTag);
        return response;
    }

    private static HttpResponseMessage CreateRangeNotSatisfiableResponse(int totalLength)
    {
        var response = new HttpResponseMessage(HttpStatusCode.RequestedRangeNotSatisfiable)
        {
            Content = new ByteArrayContent(Array.Empty<byte>())
        };
        response.Content.Headers.ContentRange = ContentRangeHeaderValue.Parse($"bytes */{totalLength}");
        return response;
    }

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

    private sealed class ScriptedHandler(
        Func<int, HttpRequestMessage, HttpResponseMessage> responseFactory) : HttpMessageHandler
    {
        private int _attempt;

        public List<RequestSnapshot> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Requests.Add(new(
                request.Headers.Range?.ToString(),
                request.Headers.IfRange?.ToString()));

            var response = responseFactory(_attempt++, request);
            response.RequestMessage = request;
            return Task.FromResult(response);
        }
    }

    private sealed record RequestSnapshot(string? Range, string? IfRange);

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

    private sealed class SlowReadStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            WaitForCancellationAsync(cancellationToken);

        private static async ValueTask<int> WaitForCancellationAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken);
            return 0;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }

    private sealed class PrefixThenStallStream(byte[] prefix) : Stream
    {
        private bool _prefixReturned;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            if (!_prefixReturned)
            {
                _prefixReturned = true;
                prefix.AsMemory().CopyTo(buffer);
                return ValueTask.FromResult(prefix.Length);
            }

            return WaitForCancellationAsync(cancellationToken);
        }

        private static async ValueTask<int> WaitForCancellationAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken);
            return 0;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }

    private sealed class PrefixThenThrowStream(byte[] prefix) : Stream
    {
        private bool _prefixReturned;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_prefixReturned)
            {
                _prefixReturned = true;
                prefix.AsMemory().CopyTo(buffer);
                return ValueTask.FromResult(prefix.Length);
            }

            return ValueTask.FromException<int>(new IOException("Falha simulada no stream."));
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
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
