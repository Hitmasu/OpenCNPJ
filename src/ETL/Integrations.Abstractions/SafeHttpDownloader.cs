namespace CNPJExporter.Integrations;

public sealed record DownloadSafetyOptions(TimeSpan Timeout, long MaxBytes)
{
    public const int DefaultBufferSize = 64 * 1024;

    public int BufferSize { get; init; } = DefaultBufferSize;

    internal void Validate()
    {
        if (Timeout <= TimeSpan.Zero || Timeout == System.Threading.Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(Timeout), "O timeout do download deve ser finito e maior que zero.");

        if (MaxBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxBytes), "O tamanho máximo do download deve ser maior que zero.");

        if (BufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(BufferSize), "O tamanho do buffer deve ser maior que zero.");
    }
}

public static class DownloadSafetyDefaults
{
    public static readonly TimeSpan RequestTimeout = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan DownloadTimeout = TimeSpan.FromHours(2);
    public const long UnknownContentLengthMaxBytes = 16L * 1024 * 1024 * 1024;

    public static DownloadSafetyOptions ForExpectedLength(long? expectedContentLength) =>
        new(
            DownloadTimeout,
            expectedContentLength is > 0
                ? expectedContentLength.Value
                : UnknownContentLengthMaxBytes);
}

public sealed record SafeDownloadResult(long BytesReceived, long? ExpectedContentLength);

public static class SafeHttpDownloader
{
    public static async Task<SafeDownloadResult> DownloadAsync(
        HttpClient httpClient,
        Uri sourceUri,
        string destinationPath,
        DownloadSafetyOptions options,
        long? expectedContentLength = null,
        Action<long, long?>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(httpClient);
        ArgumentNullException.ThrowIfNull(sourceUri);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        if (expectedContentLength is < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedContentLength),
                "O tamanho esperado do download não pode ser negativo.");
        }

        if (expectedContentLength > options.MaxBytes)
        {
            throw new InvalidDataException(
                $"O tamanho esperado do download ({expectedContentLength.Value} bytes) excede o limite configurado ({options.MaxBytes} bytes).");
        }

        var directory = Path.GetDirectoryName(destinationPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var partialPath = destinationPath + ".part";
        DeleteIfExists(partialPath);

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(options.Timeout);

        try
        {
            using var response = await httpClient.GetAsync(
                sourceUri,
                HttpCompletionOption.ResponseHeadersRead,
                deadline.Token);
            response.EnsureSuccessStatusCode();

            var responseContentLength = response.Content.Headers.ContentLength;
            if (responseContentLength > options.MaxBytes)
            {
                throw new InvalidDataException(
                    $"O servidor informou {responseContentLength.Value} bytes, acima do limite configurado de {options.MaxBytes} bytes.");
            }

            if (expectedContentLength is not null
                && responseContentLength is not null
                && responseContentLength.Value != expectedContentLength.Value)
            {
                throw new InvalidDataException(
                    $"O tamanho informado pelo servidor ({responseContentLength.Value} bytes) difere do tamanho esperado ({expectedContentLength.Value} bytes).");
            }

            var effectiveExpectedLength = expectedContentLength ?? responseContentLength;
            var bytesReceived = 0L;
            var buffer = GC.AllocateUninitializedArray<byte>(options.BufferSize);

            await using (var input = await response.Content.ReadAsStreamAsync(deadline.Token))
            await using (var output = new FileStream(
                             partialPath,
                             FileMode.CreateNew,
                             FileAccess.Write,
                             FileShare.Read,
                             options.BufferSize,
                             FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                int read;
                while ((read = await input.ReadAsync(buffer.AsMemory(), deadline.Token)) > 0)
                {
                    bytesReceived = checked(bytesReceived + read);

                    if (bytesReceived > options.MaxBytes)
                    {
                        throw new InvalidDataException(
                            $"O download excedeu o limite configurado de {options.MaxBytes} bytes.");
                    }

                    if (effectiveExpectedLength is not null
                        && bytesReceived > effectiveExpectedLength.Value)
                    {
                        throw new InvalidDataException(
                            $"O download recebeu mais dados que o esperado: {bytesReceived} de {effectiveExpectedLength.Value} bytes.");
                    }

                    await output.WriteAsync(buffer.AsMemory(0, read), deadline.Token);
                    progress?.Invoke(bytesReceived, effectiveExpectedLength);
                }

                await output.FlushAsync(deadline.Token);
            }

            if (effectiveExpectedLength is not null
                && bytesReceived != effectiveExpectedLength.Value)
            {
                throw new InvalidDataException(
                    $"Download incompleto: esperado {effectiveExpectedLength.Value} bytes, recebido {bytesReceived} bytes.");
            }

            File.Move(partialPath, destinationPath, overwrite: true);
            return new SafeDownloadResult(bytesReceived, effectiveExpectedLength);
        }
        catch (OperationCanceledException) when (
            !cancellationToken.IsCancellationRequested
            && deadline.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"O download excedeu o timeout configurado de {options.Timeout}.");
        }
        finally
        {
            DeleteIfExists(partialPath);
        }
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
