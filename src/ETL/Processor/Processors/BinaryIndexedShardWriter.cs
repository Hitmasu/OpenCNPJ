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
    private readonly FileStream _indexStream;
    private readonly UTF8Encoding _utf8NoBom = new(false);
    private bool _headerDirty = true;
    private int _recordCount;
    private long _offset;

    public BinaryIndexedShardWriter(string outputPath, string indexPath)
    {
        _dataStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        _indexStream = new FileStream(indexPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        WriteHeader();
    }

    public int RecordCount => _recordCount;

    public long DataSize => _offset;

    public async Task AppendAsync(string cnpj, string jsonData)
    {
        if (cnpj.Length != CnpjLength)
            throw new ArgumentOutOfRangeException(nameof(cnpj), $"CNPJ deve ter exatamente {CnpjLength} caracteres.");

        var line = $"{jsonData}\n";
        var bytes = _utf8NoBom.GetBytes(line);

        await _dataStream.WriteAsync(bytes);
        await WriteIndexEntryAsync(cnpj, checked((ulong)_offset), checked((uint)bytes.Length));
        _offset += bytes.Length;
        _recordCount++;
        _headerDirty = true;
    }

    public async Task FlushAsync()
    {
        await _dataStream.FlushAsync();
        RewriteHeader();
        await _indexStream.FlushAsync();
    }

    private async Task WriteIndexEntryAsync(string cnpj, ulong offset, uint length)
    {
        var entryBuffer = new byte[EntrySize];
        var cnpjBytesWritten = Encoding.ASCII.GetBytes(cnpj.AsSpan(), entryBuffer.AsSpan(0, CnpjLength));
        if (cnpjBytesWritten != CnpjLength)
            throw new InvalidOperationException($"Falha ao serializar CNPJ '{cnpj}' para índice binário.");

        BinaryPrimitives.WriteUInt64LittleEndian(entryBuffer.AsSpan(CnpjLength, sizeof(ulong)), offset);
        BinaryPrimitives.WriteUInt32LittleEndian(entryBuffer.AsSpan(CnpjLength + sizeof(ulong), sizeof(uint)), length);
        await _indexStream.WriteAsync(entryBuffer);
    }

    private void WriteHeader()
    {
        var header = new byte[HeaderSize];
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4, sizeof(uint)), checked((uint)_recordCount));
        _indexStream.Position = 0;
        _indexStream.Write(header, 0, header.Length);
        _indexStream.Position = _indexStream.Length;
        _headerDirty = false;
    }

    private void RewriteHeader()
    {
        if (!_headerDirty)
            return;

        WriteHeader();
    }

    public void Dispose()
    {
        RewriteHeader();
        _indexStream.Dispose();
        _dataStream.Dispose();
    }
}
