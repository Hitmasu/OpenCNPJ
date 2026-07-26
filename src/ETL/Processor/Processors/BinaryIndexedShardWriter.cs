using System.Buffers.Binary;
using System.Text;

namespace CNPJExporter.Processors;

internal sealed class BinaryIndexedShardWriter : IDisposable
{
    public const int CnpjLength = 14;
    public const int HeaderSize = 8;
    public const int EntrySize = CnpjLength + sizeof(ulong) + sizeof(uint);
    private const int StreamBufferSize = 64 * 1024;

    private static ReadOnlySpan<byte> Magic => "OCI1"u8;

    private readonly string _outputPath;
    private readonly string _indexPath;
    private readonly UTF8Encoding _utf8NoBom = new(false);
    private readonly List<IndexEntry> _indexEntries = [];
    private FileStream? _dataStream;
    private StreamWriter? _dataWriter;
    private bool _indexDirty = true;
    private bool _disposed;
    private int _recordCount;
    private long _offset;

    public BinaryIndexedShardWriter(string outputPath, string indexPath)
    {
        _outputPath = outputPath;
        _indexPath = indexPath;
        using (new FileStream(
            _outputPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.Read))
        {
        }

        RewriteIndex();
    }

    public int RecordCount => _recordCount;

    public long DataSize => _offset;

    internal bool IsOpen => _dataWriter is not null;

    public async Task AppendAsync(string cnpj, string jsonData)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (cnpj.Length != CnpjLength)
            throw new ArgumentOutOfRangeException(nameof(cnpj), $"CNPJ deve ter exatamente {CnpjLength} caracteres.");

        var byteLength = checked((uint)(_utf8NoBom.GetByteCount(jsonData) + 1));
        EnsureDataWriter();

        await _dataWriter!.WriteAsync(jsonData);
        await _dataWriter.WriteAsync('\n');
        _indexEntries.Add(CreateIndexEntry(cnpj, checked((ulong)_offset), byteLength));
        _offset += byteLength;
        _recordCount++;
        _indexDirty = true;
    }

    public async Task FlushAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await SuspendAsync();
        RewriteIndex();
    }

    internal async Task SuspendAsync()
    {
        if (_dataWriter is null)
            return;

        await _dataWriter.FlushAsync();
        _dataWriter.Dispose();
        _dataStream?.Dispose();
        _dataWriter = null;
        _dataStream = null;
    }

    private void EnsureDataWriter()
    {
        if (_dataWriter is not null)
            return;

        _dataStream = new FileStream(
            _outputPath,
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read,
            StreamBufferSize,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        _dataWriter = new StreamWriter(
            _dataStream,
            _utf8NoBom,
            StreamBufferSize,
            leaveOpen: true);
    }

    private static IndexEntry CreateIndexEntry(string cnpj, ulong offset, uint length)
    {
        Span<byte> cnpjBytes = stackalloc byte[CnpjLength];
        var cnpjBytesWritten = Encoding.ASCII.GetBytes(cnpj.AsSpan(), cnpjBytes);
        if (cnpjBytesWritten != CnpjLength)
            throw new InvalidOperationException($"Falha ao serializar CNPJ '{cnpj}' para índice binário.");

        Span<byte> secondBlock = stackalloc byte[sizeof(ulong)];
        secondBlock.Clear();
        cnpjBytes[8..].CopyTo(secondBlock);
        return new IndexEntry(
            BinaryPrimitives.ReadUInt64LittleEndian(cnpjBytes[..8]),
            BinaryPrimitives.ReadUInt64LittleEndian(secondBlock),
            offset,
            length);
    }

    private static void WriteIndexEntry(Stream indexStream, IndexEntry entry)
    {
        Span<byte> entryBuffer = stackalloc byte[EntrySize];
        BinaryPrimitives.WriteUInt64LittleEndian(entryBuffer[..8], entry.CnpjFirstBlock);

        Span<byte> secondBlock = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(secondBlock, entry.CnpjSecondBlock);
        secondBlock[..6].CopyTo(entryBuffer[8..CnpjLength]);

        BinaryPrimitives.WriteUInt64LittleEndian(entryBuffer.Slice(CnpjLength, sizeof(ulong)), entry.Offset);
        BinaryPrimitives.WriteUInt32LittleEndian(entryBuffer.Slice(CnpjLength + sizeof(ulong), sizeof(uint)), entry.Length);
        indexStream.Write(entryBuffer);
    }

    private void WriteHeader(Stream indexStream)
    {
        var header = new byte[HeaderSize];
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4, sizeof(uint)), checked((uint)_recordCount));
        indexStream.Write(header, 0, header.Length);
    }

    private void RewriteIndex()
    {
        if (!_indexDirty)
            return;

        using var indexStream = new FileStream(
            _indexPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.Read,
            StreamBufferSize,
            FileOptions.SequentialScan);
        WriteHeader(indexStream);

        _indexEntries.Sort(CompareIndexEntries);
        foreach (var entry in _indexEntries)
            WriteIndexEntry(indexStream, entry);

        _indexDirty = false;
    }

    private static int CompareIndexEntries(IndexEntry left, IndexEntry right)
    {
        var result = ComparePackedAscii(left.CnpjFirstBlock, right.CnpjFirstBlock, sizeof(ulong));
        return result != 0
            ? result
            : ComparePackedAscii(left.CnpjSecondBlock, right.CnpjSecondBlock, CnpjLength - sizeof(ulong));
    }

    private static int ComparePackedAscii(ulong left, ulong right, int byteCount)
    {
        for (var index = 0; index < byteCount; index++)
        {
            var shift = index * 8;
            var diff = (int)((left >> shift) & 0xFF) - (int)((right >> shift) & 0xFF);
            if (diff != 0)
                return diff;
        }

        return 0;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        try
        {
            _dataWriter?.Flush();
            _dataWriter?.Dispose();
            _dataStream?.Dispose();
            RewriteIndex();
        }
        finally
        {
            _disposed = true;
        }
    }

    private readonly record struct IndexEntry(ulong CnpjFirstBlock, ulong CnpjSecondBlock, ulong Offset, uint Length);
}
