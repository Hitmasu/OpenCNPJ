using System.Text;

namespace CNPJExporter.Processors;

internal sealed class SparseShardWriter : IDisposable
{
    private readonly FileStream _stream;
    private readonly string _prefix;
    private readonly int _stride;
    private readonly List<SparseShardIndexEntry> _entries = [];
    private readonly UTF8Encoding _utf8NoBom = new(false);
    private int _recordCount;
    private long _offset;

    public SparseShardWriter(string prefix, string outputPath, int stride)
    {
        _prefix = prefix;
        _stride = Math.Max(1, stride);
        _stream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.Read);
    }

    public int RecordCount => _recordCount;

    public long DataSize => _offset;

    public async Task AppendAsync(string cnpj, string jsonData)
    {
        if (_recordCount == 0 || _recordCount % _stride == 0)
        {
            _entries.Add(new SparseShardIndexEntry
            {
                Cnpj = cnpj,
                Offset = _offset
            });
        }

        var line = $"{{\"cnpj\":\"{cnpj}\",\"data\":{jsonData}}}\n";
        var bytes = _utf8NoBom.GetBytes(line);
        await _stream.WriteAsync(bytes);
        _offset += bytes.Length;
        _recordCount++;
    }

    public async Task FlushAsync()
    {
        await _stream.FlushAsync();
    }

    public SparseShardIndexDocument BuildIndexDocument(string dataFileName)
    {
        return new SparseShardIndexDocument
        {
            Format = "ndjson",
            Prefix = _prefix,
            DataFile = dataFileName,
            Stride = _stride,
            RecordCount = _recordCount,
            DataSize = _offset,
            Entries = [.. _entries]
        };
    }

    public void Dispose()
    {
        _stream.Dispose();
    }
}
