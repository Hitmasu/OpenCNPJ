using System.Buffers.Binary;
using System.Text;

namespace CNPJExporter.Processors;

internal sealed class BinaryIndexedShardWriter : IDisposable
{
    public const int CnpjLength = 14;
    public const int HeaderSize = 8;
    public const int EntrySize = CnpjLength + sizeof(ulong) + sizeof(uint);

    private static ReadOnlySpan<byte> Magic => "OCI1"u8;

    private readonly FileStream _dataStream;
    private readonly StreamWriter _dataWriter;
    private readonly FileStream _indexStream;
    private readonly UTF8Encoding _utf8NoBom = new(false);
    private readonly List<IndexEntry> _indexEntries = [];
    private bool _indexDirty = true;
    private int _recordCount;
    private long _offset;

    public BinaryIndexedShardWriter(string outputPath, string indexPath)
    {
        _dataStream = new FileStream(
            outputPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 1024 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        _dataWriter = new StreamWriter(_dataStream, _utf8NoBom, bufferSize: 1024 * 1024, leaveOpen: true);
        _indexStream = new FileStream(indexPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        RewriteIndex();
    }

    public int RecordCount => _recordCount;

    public long DataSize => _offset;

    public async Task AppendAsync(string cnpj, string jsonData)
    {
        if (cnpj.Length != CnpjLength)
            throw new ArgumentOutOfRangeException(nameof(cnpj), $"CNPJ deve ter exatamente {CnpjLength} caracteres.");

        var byteLength = checked((uint)(_utf8NoBom.GetByteCount(jsonData) + 1));

        await _dataWriter.WriteAsync(jsonData);
        await _dataWriter.WriteAsync('\n');
        _indexEntries.Add(CreateIndexEntry(cnpj, checked((ulong)_offset), byteLength));
        _offset += byteLength;
        _recordCount++;
        _indexDirty = true;
    }

    public async Task FlushAsync()
    {
        await _dataWriter.FlushAsync();
        RewriteIndex();
        await _indexStream.FlushAsync();
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

    private void WriteIndexEntry(IndexEntry entry)
    {
        Span<byte> entryBuffer = stackalloc byte[EntrySize];
        BinaryPrimitives.WriteUInt64LittleEndian(entryBuffer[..8], entry.CnpjFirstBlock);

        Span<byte> secondBlock = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(secondBlock, entry.CnpjSecondBlock);
        secondBlock[..6].CopyTo(entryBuffer[8..CnpjLength]);

        BinaryPrimitives.WriteUInt64LittleEndian(entryBuffer.Slice(CnpjLength, sizeof(ulong)), entry.Offset);
        BinaryPrimitives.WriteUInt32LittleEndian(entryBuffer.Slice(CnpjLength + sizeof(ulong), sizeof(uint)), entry.Length);
        _indexStream.Write(entryBuffer);
    }

    private void WriteHeader()
    {
        var header = new byte[HeaderSize];
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4, sizeof(uint)), checked((uint)_recordCount));
        _indexStream.Write(header, 0, header.Length);
    }

    private void RewriteIndex()
    {
        if (!_indexDirty)
            return;

        _indexStream.Position = 0;
        _indexStream.SetLength(0);
        WriteHeader();

        _indexEntries.Sort(CompareIndexEntries);
        foreach (var entry in _indexEntries)
            WriteIndexEntry(entry);

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
        _dataWriter.Flush();
        RewriteIndex();
        _indexStream.Dispose();
        _dataWriter.Dispose();
        _dataStream.Dispose();
    }

    private readonly record struct IndexEntry(ulong CnpjFirstBlock, ulong CnpjSecondBlock, ulong Offset, uint Length);
}
