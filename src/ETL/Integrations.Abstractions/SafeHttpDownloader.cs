using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace CNPJExporter.Integrations;

public sealed record DownloadSafetyOptions(TimeSpan Timeout, long MaxBytes)
{
    public const int DefaultBufferSize = 64 * 1024;

    public int BufferSize { get; init; } = DefaultBufferSize;

    public TimeSpan InactivityTimeout { get; init; } = DownloadSafetyDefaults.StreamInactivityTimeout;

    internal void Validate()
    {
        if (Timeout <= TimeSpan.Zero || Timeout == System.Threading.Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(Timeout), "O timeout do download deve ser finito e maior que zero.");

        if (InactivityTimeout <= TimeSpan.Zero || InactivityTimeout == System.Threading.Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(
                nameof(InactivityTimeout),
                "O timeout de inatividade deve ser finito e maior que zero.");
        }

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
    public static readonly TimeSpan StreamInactivityTimeout = TimeSpan.FromMinutes(2);
    public const long UnknownContentLengthMaxBytes = 16L * 1024 * 1024 * 1024;

    public static DownloadSafetyOptions ForExpectedLength(long? expectedContentLength) =>
        new(
            DownloadTimeout,
            expectedContentLength is > 0
                ? expectedContentLength.Value
                : UnknownContentLengthMaxBytes)
        {
            InactivityTimeout = StreamInactivityTimeout
        };
}

public sealed record SafeDownloadResult(long BytesReceived, long? ExpectedContentLength);

public static class SafeHttpDownloader
{
    private const int ResumeMetadataVersion = 1;

    public static async Task<SafeDownloadResult> DownloadAsync(
        HttpClient httpClient,
        Uri sourceUri,
        string destinationPath,
        DownloadSafetyOptions options,
        long? expectedContentLength = null,
        Action<long, long?>? progress = null,
        string? entityTag = null,
        DateTimeOffset? lastModified = null,
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
        var metadataPath = partialPath + ".meta";
        var currentMetadata = CreateMetadata(sourceUri, expectedContentLength, entityTag, lastModified);
        var resumeState = await PrepareResumeStateAsync(
            partialPath,
            metadataPath,
            currentMetadata,
            options,
            cancellationToken);

        var resumeOffset = resumeState.Offset;
        var resumeMetadata = resumeState.Metadata;
        var fullFallbackAttempted = false;

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(options.Timeout);

        try
        {
            while (true)
            {
                var requestedOffset = resumeOffset;

                using var request = new HttpRequestMessage(HttpMethod.Get, sourceUri);
                if (requestedOffset > 0)
                {
                    request.Headers.Range = new RangeHeaderValue(requestedOffset, null);

                    var ifRange = CreateIfRange(resumeMetadata);
                    if (ifRange is not null)
                        request.Headers.IfRange = ifRange;
                }

                using var response = await httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    deadline.Token);

                if (requestedOffset > 0
                    && response.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable)
                {
                    var remoteLength = response.Content.Headers.ContentRange?.Length;
                    var expectedMatches = expectedContentLength is null || remoteLength == expectedContentLength;

                    if (remoteLength == requestedOffset
                        && expectedMatches
                        && CanTrustRangeCompletion(resumeMetadata, currentMetadata))
                    {
                        PublishPartialFile(partialPath, metadataPath, destinationPath);
                        return new SafeDownloadResult(requestedOffset, remoteLength);
                    }

                    if (fullFallbackAttempted)
                    {
                        throw new InvalidDataException(
                            $"O servidor rejeitou o range solicitado a partir do byte {requestedOffset}.");
                    }

                    ResetPartialFiles(partialPath, metadataPath);
                    resumeOffset = 0;
                    resumeMetadata = currentMetadata;
                    fullFallbackAttempted = true;
                    continue;
                }

                response.EnsureSuccessStatusCode();

                var wasResumeRequest = requestedOffset > 0;

                if (wasResumeRequest && response.StatusCode == HttpStatusCode.PartialContent)
                {
                    if (!TryValidatePartialResponse(
                            response,
                            requestedOffset,
                            expectedContentLength,
                            out var rangeTotalLength,
                            out var validationError)
                        || ValidatorsConflict(resumeMetadata, response))
                    {
                        if (fullFallbackAttempted)
                        {
                            throw new InvalidDataException(
                                validationError ?? "O servidor retornou um partial content incompatível com o arquivo parcial local.");
                        }

                        ResetPartialFiles(partialPath, metadataPath);
                        resumeOffset = 0;
                        resumeMetadata = currentMetadata;
                        fullFallbackAttempted = true;
                        continue;
                    }

                    resumeOffset = requestedOffset;
                }
                else if (wasResumeRequest && response.StatusCode == HttpStatusCode.OK)
                {
                    // O servidor ignorou Range ou o If-Range não corresponde mais ao recurso atual.
                    // Em ambos os casos, reiniciamos usando a resposta completa recebida.
                    ResetPartialFiles(partialPath, metadataPath);
                    resumeOffset = 0;
                }
                else if (wasResumeRequest)
                {
                    throw new InvalidDataException(
                        $"Resposta inesperada ao retomar download: HTTP {(int)response.StatusCode} ({response.StatusCode}).");
                }
                else if (response.StatusCode == HttpStatusCode.PartialContent)
                {
                    throw new InvalidDataException(
                        "O servidor retornou 206 Partial Content sem que um Range tivesse sido solicitado.");
                }

                var responseContentLength = response.Content.Headers.ContentLength;
                var effectiveExpectedLength = ResolveExpectedLength(
                    response,
                    resumeOffset,
                    expectedContentLength,
                    responseContentLength);

                ValidateResponseLengths(
                    response,
                    resumeOffset,
                    expectedContentLength,
                    effectiveExpectedLength,
                    responseContentLength,
                    options.MaxBytes);

                var replaceStoredValidators = wasResumeRequest && response.StatusCode == HttpStatusCode.OK;
                resumeMetadata = CreateMetadataFromResponse(
                    currentMetadata,
                    resumeMetadata,
                    response,
                    effectiveExpectedLength,
                    replaceStoredValidators);

                await SaveResumeMetadataAsync(metadataPath, resumeMetadata, deadline.Token);

                var bytesReceived = resumeOffset;
                progress?.Invoke(bytesReceived, effectiveExpectedLength);

                var fileMode = resumeOffset > 0 ? FileMode.Append : FileMode.Create;
                var buffer = GC.AllocateUninitializedArray<byte>(options.BufferSize);

                await using (var input = await response.Content.ReadAsStreamAsync(deadline.Token))
                await using (var output = new FileStream(
                                 partialPath,
                                 fileMode,
                                 FileAccess.Write,
                                 FileShare.Read,
                                 options.BufferSize,
                                 FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    int read;
                    while ((read = await ReadWithInactivityTimeoutAsync(
                                input,
                                buffer.AsMemory(),
                                options.InactivityTimeout,
                                deadline.Token,
                                cancellationToken)) > 0)
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
                    throw new IOException(
                        $"Download incompleto: esperado {effectiveExpectedLength.Value} bytes, recebido {bytesReceived} bytes.");
                }

                PublishPartialFile(partialPath, metadataPath, destinationPath);
                return new SafeDownloadResult(bytesReceived, effectiveExpectedLength);
            }
        }
        catch (OperationCanceledException ex) when (
            !cancellationToken.IsCancellationRequested
            && deadline.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"O download excedeu o timeout configurado de {options.Timeout}.",
                ex);
        }
        catch (InvalidDataException)
        {
            ResetPartialFiles(partialPath, metadataPath);
            throw;
        }
        finally
        {
            CleanupEmptyPartial(partialPath, metadataPath);
        }
    }

    private static async ValueTask<int> ReadWithInactivityTimeoutAsync(
        Stream input,
        Memory<byte> buffer,
        TimeSpan inactivityTimeout,
        CancellationToken deadlineToken,
        CancellationToken callerToken)
    {
        using var inactivity = CancellationTokenSource.CreateLinkedTokenSource(deadlineToken);
        inactivity.CancelAfter(inactivityTimeout);

        try
        {
            return await input.ReadAsync(buffer, inactivity.Token);
        }
        catch (OperationCanceledException ex) when (
            !callerToken.IsCancellationRequested
            && !deadlineToken.IsCancellationRequested
            && inactivity.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Nenhum dado foi recebido durante {inactivityTimeout}.",
                ex);
        }
    }

    private static long? ResolveExpectedLength(
        HttpResponseMessage response,
        long resumeOffset,
        long? expectedContentLength,
        long? responseContentLength)
    {
        if (expectedContentLength is not null)
            return expectedContentLength;

        if (response.StatusCode == HttpStatusCode.PartialContent)
        {
            var rangeLength = response.Content.Headers.ContentRange?.Length;
            if (rangeLength is not null)
                return rangeLength;

            if (responseContentLength is not null)
                return checked(resumeOffset + responseContentLength.Value);
        }

        return responseContentLength;
    }

    private static void ValidateResponseLengths(
        HttpResponseMessage response,
        long resumeOffset,
        long? expectedContentLength,
        long? effectiveExpectedLength,
        long? responseContentLength,
        long maxBytes)
    {
        if (effectiveExpectedLength > maxBytes)
        {
            throw new InvalidDataException(
                $"O tamanho total do download ({effectiveExpectedLength.Value} bytes) excede o limite configurado ({maxBytes} bytes).");
        }

        if (responseContentLength is not null
            && responseContentLength.Value > maxBytes - resumeOffset)
        {
            throw new InvalidDataException(
                $"O servidor informou {responseContentLength.Value} bytes adicionais, acima do limite restante configurado.");
        }

        if (response.StatusCode == HttpStatusCode.OK
            && expectedContentLength is not null
            && responseContentLength is not null
            && responseContentLength.Value != expectedContentLength.Value)
        {
            throw new InvalidDataException(
                $"O tamanho informado pelo servidor ({responseContentLength.Value} bytes) difere do tamanho esperado ({expectedContentLength.Value} bytes).");
        }

        if (response.StatusCode == HttpStatusCode.PartialContent
            && expectedContentLength is not null
            && responseContentLength is not null
            && responseContentLength.Value > expectedContentLength.Value - resumeOffset)
        {
            throw new InvalidDataException(
                "O servidor retornou mais bytes do que o restante esperado para o arquivo.");
        }
    }

    private static bool TryValidatePartialResponse(
        HttpResponseMessage response,
        long requestedOffset,
        long? expectedContentLength,
        out long? totalLength,
        out string? error)
    {
        totalLength = null;
        error = null;

        var contentRange = response.Content.Headers.ContentRange;
        if (contentRange is null
            || !string.Equals(contentRange.Unit, "bytes", StringComparison.OrdinalIgnoreCase)
            || contentRange.From != requestedOffset
            || contentRange.To is null)
        {
            error = "Content-Range ausente ou incompatível com o offset solicitado.";
            return false;
        }

        totalLength = contentRange.Length;

        if (expectedContentLength is not null
            && totalLength is not null
            && totalLength.Value != expectedContentLength.Value)
        {
            error = $"O total informado em Content-Range ({totalLength.Value}) difere do tamanho esperado ({expectedContentLength.Value}).";
            return false;
        }

        if (expectedContentLength is not null
            && contentRange.To.Value >= expectedContentLength.Value)
        {
            error = "Content-Range ultrapassa o tamanho esperado do arquivo.";
            return false;
        }

        var responseContentLength = response.Content.Headers.ContentLength;
        var rangeLength = checked(contentRange.To.Value - contentRange.From.Value + 1);
        if (responseContentLength is not null && responseContentLength.Value != rangeLength)
        {
            error = "Content-Length não corresponde ao intervalo informado em Content-Range.";
            return false;
        }

        return true;
    }

    private static bool ValidatorsConflict(
        DownloadResumeMetadata metadata,
        HttpResponseMessage response)
    {
        var responseEntityTag = NormalizeEntityTag(response.Headers.ETag?.ToString());
        if (metadata.EntityTag is not null
            && responseEntityTag is not null
            && !string.Equals(metadata.EntityTag, responseEntityTag, StringComparison.Ordinal))
        {
            return true;
        }

        if (metadata.EntityTag is null
            && metadata.LastModified is not null
            && response.Content.Headers.LastModified is not null
            && metadata.LastModified.Value != response.Content.Headers.LastModified.Value.ToUniversalTime())
        {
            return true;
        }

        return false;
    }

    private static DownloadResumeMetadata CreateMetadata(
        Uri sourceUri,
        long? expectedContentLength,
        string? entityTag,
        DateTimeOffset? lastModified) =>
        new()
        {
            Version = ResumeMetadataVersion,
            SourceUri = sourceUri.AbsoluteUri,
            ExpectedContentLength = expectedContentLength,
            EntityTag = NormalizeEntityTag(entityTag),
            LastModified = lastModified?.ToUniversalTime()
        };

    private static DownloadResumeMetadata CreateMetadataFromResponse(
        DownloadResumeMetadata currentMetadata,
        DownloadResumeMetadata resumeMetadata,
        HttpResponseMessage response,
        long? expectedContentLength,
        bool replaceStoredValidators)
    {
        var responseEntityTag = NormalizeEntityTag(response.Headers.ETag?.ToString());
        var responseLastModified = response.Content.Headers.LastModified?.ToUniversalTime();

        var fallbackMetadata = replaceStoredValidators ? currentMetadata : resumeMetadata;

        return new DownloadResumeMetadata
        {
            Version = ResumeMetadataVersion,
            SourceUri = currentMetadata.SourceUri,
            ExpectedContentLength = expectedContentLength ?? fallbackMetadata.ExpectedContentLength,
            EntityTag = responseEntityTag
                ?? (replaceStoredValidators ? null : fallbackMetadata.EntityTag),
            LastModified = responseLastModified
                ?? (replaceStoredValidators ? null : fallbackMetadata.LastModified)
        };
    }

    private static async Task<ResumeState> PrepareResumeStateAsync(
        string partialPath,
        string metadataPath,
        DownloadResumeMetadata currentMetadata,
        DownloadSafetyOptions options,
        CancellationToken cancellationToken)
    {
        if (!File.Exists(partialPath))
        {
            DeleteIfExists(metadataPath);
            return new ResumeState(0, currentMetadata);
        }

        var partialLength = new FileInfo(partialPath).Length;
        if (partialLength <= 0
            || partialLength > options.MaxBytes
            || (currentMetadata.ExpectedContentLength is not null
                && partialLength > currentMetadata.ExpectedContentLength.Value))
        {
            ResetPartialFiles(partialPath, metadataPath);
            return new ResumeState(0, currentMetadata);
        }

        if (!File.Exists(metadataPath))
        {
            ResetPartialFiles(partialPath, metadataPath);
            return new ResumeState(0, currentMetadata);
        }

        DownloadResumeMetadata? storedMetadata;
        try
        {
            var json = await File.ReadAllTextAsync(metadataPath, cancellationToken);
            storedMetadata = JsonSerializer.Deserialize<DownloadResumeMetadata>(json);
        }
        catch (JsonException)
        {
            ResetPartialFiles(partialPath, metadataPath);
            return new ResumeState(0, currentMetadata);
        }

        if (storedMetadata is null
            || storedMetadata.Version != ResumeMetadataVersion
            || !CanResume(storedMetadata, currentMetadata))
        {
            ResetPartialFiles(partialPath, metadataPath);
            return new ResumeState(0, currentMetadata);
        }

        return new ResumeState(
            partialLength,
            new DownloadResumeMetadata
            {
                Version = ResumeMetadataVersion,
                SourceUri = storedMetadata.SourceUri,
                ExpectedContentLength = currentMetadata.ExpectedContentLength
                    ?? storedMetadata.ExpectedContentLength,
                EntityTag = storedMetadata.EntityTag,
                LastModified = storedMetadata.LastModified
            });
    }

    private static bool CanResume(
        DownloadResumeMetadata storedMetadata,
        DownloadResumeMetadata currentMetadata)
    {
        if (!string.Equals(
                storedMetadata.SourceUri,
                currentMetadata.SourceUri,
                StringComparison.Ordinal))
        {
            return false;
        }

        if (storedMetadata.ExpectedContentLength is not null
            && currentMetadata.ExpectedContentLength is not null
            && storedMetadata.ExpectedContentLength.Value != currentMetadata.ExpectedContentLength.Value)
        {
            return false;
        }

        if (currentMetadata.EntityTag is not null)
        {
            return storedMetadata.EntityTag is not null
                && string.Equals(
                    storedMetadata.EntityTag,
                    currentMetadata.EntityTag,
                    StringComparison.Ordinal);
        }

        if (storedMetadata.EntityTag is not null)
            return true;

        if (currentMetadata.LastModified is not null)
        {
            return storedMetadata.LastModified is not null
                && storedMetadata.LastModified.Value == currentMetadata.LastModified.Value;
        }

        return true;
    }

    private static bool CanTrustRangeCompletion(
        DownloadResumeMetadata resumeMetadata,
        DownloadResumeMetadata currentMetadata)
    {
        if (CreateIfRange(resumeMetadata) is not null)
            return true;

        if (currentMetadata.EntityTag is not null
            && resumeMetadata.EntityTag is not null
            && string.Equals(
                currentMetadata.EntityTag,
                resumeMetadata.EntityTag,
                StringComparison.Ordinal))
        {
            return true;
        }

        return currentMetadata.LastModified is not null
            && resumeMetadata.LastModified is not null
            && currentMetadata.LastModified.Value == resumeMetadata.LastModified.Value;
    }

    private static RangeConditionHeaderValue? CreateIfRange(DownloadResumeMetadata metadata)
    {
        if (metadata.EntityTag is not null
            && EntityTagHeaderValue.TryParse(metadata.EntityTag, out var entityTag)
            && !entityTag.IsWeak)
        {
            return new RangeConditionHeaderValue(entityTag);
        }

        return metadata.LastModified is not null
            ? new RangeConditionHeaderValue(metadata.LastModified.Value)
            : null;
    }

    private static string? NormalizeEntityTag(string? entityTag)
    {
        if (string.IsNullOrWhiteSpace(entityTag))
            return null;

        var candidate = entityTag.Trim();
        if (EntityTagHeaderValue.TryParse(candidate, out var parsed))
            return parsed.ToString();

        candidate = $"\"{candidate.Trim('"')}\"";
        return EntityTagHeaderValue.TryParse(candidate, out parsed)
            ? parsed.ToString()
            : null;
    }

    private static async Task SaveResumeMetadataAsync(
        string metadataPath,
        DownloadResumeMetadata metadata,
        CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(metadata);
        await File.WriteAllTextAsync(metadataPath, json, cancellationToken);
    }

    private static void PublishPartialFile(
        string partialPath,
        string metadataPath,
        string destinationPath)
    {
        File.Move(partialPath, destinationPath, overwrite: true);
        DeleteIfExists(metadataPath);
    }

    private static void ResetPartialFiles(string partialPath, string metadataPath)
    {
        DeleteIfExists(partialPath);
        DeleteIfExists(metadataPath);
    }

    private static void CleanupEmptyPartial(string partialPath, string metadataPath)
    {
        if (!File.Exists(partialPath))
        {
            DeleteIfExists(metadataPath);
            return;
        }

        if (new FileInfo(partialPath).Length == 0)
            ResetPartialFiles(partialPath, metadataPath);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed record ResumeState(long Offset, DownloadResumeMetadata Metadata);

    private sealed class DownloadResumeMetadata
    {
        public int Version { get; init; }

        public string SourceUri { get; init; } = string.Empty;

        public long? ExpectedContentLength { get; init; }

        public string? EntityTag { get; init; }

        public DateTimeOffset? LastModified { get; init; }
    }
}
