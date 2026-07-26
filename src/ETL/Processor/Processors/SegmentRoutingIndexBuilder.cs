using System.Buffers.Binary;
using System.Text;
using CNPJExporter.Processors.Models;

namespace CNPJExporter.Processors;

internal sealed class SegmentRoutingIndexBuilder
{
    private const int HeaderSize = 8;
    private const int CnpjLength = BinaryIndexedShardWriter.CnpjLength;
    private static ReadOnlySpan<byte> Magic => "OCR1"u8;
    private static ReadOnlySpan<byte> ShardIndexMagic => "OCI1"u8;
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    private readonly Dictionary<
        string,
        SortedDictionary<string, List<SegmentRoutingReference>>> _routes =
        new(StringComparer.Ordinal);

    public IReadOnlyCollection<string> Prefixes => _routes.Keys;

    public void LoadDirectory(string routingDirectory)
    {
        if (!Directory.Exists(routingDirectory))
            return;

        foreach (var path in Directory
                     .EnumerateFiles(
                         routingDirectory,
                         "*.routing.bin",
                         SearchOption.TopDirectoryOnly)
                     .OrderBy(path => path, StringComparer.Ordinal))
        {
            var prefix = Path.GetFileName(path)[
                ..^".routing.bin".Length];
            _routes[prefix] = ReadRoutingFile(path);
        }
    }

    public void RemoveSegment(string segmentId)
    {
        ValidateSegmentId(segmentId);

        foreach (var routesByCnpj in _routes.Values)
        {
            foreach (var cnpj in routesByCnpj.Keys.ToArray())
            {
                routesByCnpj[cnpj].RemoveAll(reference =>
                    string.Equals(
                        reference.SegmentId,
                        segmentId,
                        StringComparison.Ordinal));
                if (routesByCnpj[cnpj].Count == 0)
                    routesByCnpj.Remove(cnpj);
            }
        }
    }

    public void AddSegment(string segmentId, string shardDirectory)
    {
        ValidateSegmentId(segmentId);
        if (!Directory.Exists(shardDirectory))
        {
            throw new DirectoryNotFoundException(
                $"Diretório de shards do segmento não encontrado: {shardDirectory}");
        }

        foreach (var indexPath in Directory
                     .EnumerateFiles(
                         shardDirectory,
                         "*.index.bin",
                         SearchOption.TopDirectoryOnly)
                     .OrderBy(path => path, StringComparer.Ordinal))
        {
            var prefix = Path.GetFileName(indexPath)[
                ..^".index.bin".Length];
            if (!_routes.TryGetValue(prefix, out var routesByCnpj))
            {
                routesByCnpj = new SortedDictionary<
                    string,
                    List<SegmentRoutingReference>>(StringComparer.Ordinal);
                _routes[prefix] = routesByCnpj;
            }

            foreach (var entriesByCnpj in ReadShardIndex(indexPath)
                         .GroupBy(entry => entry.Cnpj, StringComparer.Ordinal))
            {
                var cnpj = entriesByCnpj.Key;
                if (!routesByCnpj.TryGetValue(cnpj, out var references))
                {
                    references = [];
                    routesByCnpj[cnpj] = references;
                }

                foreach (var (offset, length) in CoalesceAdjacentEntries(
                             entriesByCnpj
                                 .Select(entry => (entry.Offset, entry.Length))
                                 .OrderBy(entry => entry.Offset)))
                {
                    references.Add(new SegmentRoutingReference(
                        segmentId,
                        offset,
                        length));
                }

                references.Sort((left, right) =>
                {
                    var segmentComparison = string.CompareOrdinal(
                        left.SegmentId,
                        right.SegmentId);
                    return segmentComparison != 0
                        ? segmentComparison
                        : left.Offset.CompareTo(right.Offset);
                });
            }
        }
    }

    private static IEnumerable<(ulong Offset, uint Length)>
        CoalesceAdjacentEntries(
            IEnumerable<(ulong Offset, uint Length)> orderedEntries)
    {
        using var enumerator = orderedEntries.GetEnumerator();
        if (!enumerator.MoveNext())
            yield break;

        var current = enumerator.Current;
        while (enumerator.MoveNext())
        {
            var next = enumerator.Current;
            var currentEnd = checked(current.Offset + current.Length);
            var combinedLength = (ulong)current.Length + next.Length;
            if (currentEnd == next.Offset && combinedLength <= uint.MaxValue)
            {
                current = (current.Offset, checked((uint)combinedLength));
                continue;
            }

            yield return current;
            current = next;
        }

        yield return current;
    }

    public IReadOnlyList<string> WriteDirectory(string outputDirectory)
    {
        Directory.CreateDirectory(outputDirectory);
        foreach (var oldPath in Directory.EnumerateFiles(
                     outputDirectory,
                     "*.routing.bin",
                     SearchOption.TopDirectoryOnly))
        {
            File.Delete(oldPath);
        }

        var written = new List<string>();
        foreach (var (prefix, routesByCnpj) in _routes
                     .Where(entry => entry.Value.Count > 0)
                     .OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            var outputPath = Path.Combine(
                outputDirectory,
                $"{prefix}.routing.bin");
            WriteRoutingFile(outputPath, routesByCnpj);
            written.Add(prefix);
        }

        return written;
    }

    internal IReadOnlyList<SegmentRoutingReference> GetReferencesForTest(
        string prefix,
        string cnpj)
    {
        if (!_routes.TryGetValue(prefix, out var routesByCnpj)
            || !routesByCnpj.TryGetValue(cnpj, out var references))
        {
            return [];
        }

        return references;
    }

    private static void WriteRoutingFile(
        string path,
        SortedDictionary<string, List<SegmentRoutingReference>> routesByCnpj)
    {
        using var stream = new FileStream(
            path,
            FileMode.Create,
            FileAccess.Write,
            FileShare.Read);

        Span<byte> header = stackalloc byte[HeaderSize];
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt32LittleEndian(
            header[4..],
            checked((uint)routesByCnpj.Count));
        stream.Write(header);

        var cnpjBytes = new byte[CnpjLength];
        var referenceCountBytes = new byte[sizeof(ushort)];
        var rangeBytes = new byte[sizeof(ulong) + sizeof(uint)];
        foreach (var (cnpj, references) in routesByCnpj)
        {
            var cnpjBytesWritten = Encoding.ASCII.GetBytes(
                cnpj.AsSpan(),
                cnpjBytes.AsSpan());
            if (cnpjBytesWritten != CnpjLength)
            {
                throw new InvalidDataException(
                    $"CNPJ inválido no roteamento: {cnpj}");
            }

            stream.Write(cnpjBytes);
            BinaryPrimitives.WriteUInt16LittleEndian(
                referenceCountBytes,
                checked((ushort)references.Count));
            stream.Write(referenceCountBytes);

            foreach (var reference in references)
            {
                var segmentId = StrictUtf8.GetBytes(reference.SegmentId);
                stream.WriteByte(checked((byte)segmentId.Length));
                stream.Write(segmentId);

                BinaryPrimitives.WriteUInt64LittleEndian(
                    rangeBytes.AsSpan(0, sizeof(ulong)),
                    reference.Offset);
                BinaryPrimitives.WriteUInt32LittleEndian(
                    rangeBytes.AsSpan(sizeof(ulong)),
                    reference.Length);
                stream.Write(rangeBytes);
            }
        }
    }

    private static SortedDictionary<string, List<SegmentRoutingReference>>
        ReadRoutingFile(string path)
    {
        var bytes = File.ReadAllBytes(path);
        if (bytes.Length < HeaderSize
            || !bytes.AsSpan(0, Magic.Length).SequenceEqual(Magic))
        {
            throw new InvalidDataException(
                $"Índice de roteamento inválido: {path}");
        }

        var recordCount = BinaryPrimitives.ReadUInt32LittleEndian(
            bytes.AsSpan(4, sizeof(uint)));
        var routes = new SortedDictionary<
            string,
            List<SegmentRoutingReference>>(StringComparer.Ordinal);
        var cursor = HeaderSize;

        for (var recordIndex = 0; recordIndex < recordCount; recordIndex++)
        {
            EnsureAvailable(bytes, cursor, CnpjLength + sizeof(ushort), path);
            var cnpj = Encoding.ASCII.GetString(
                bytes,
                cursor,
                CnpjLength);
            cursor += CnpjLength;
            var referenceCount = BinaryPrimitives.ReadUInt16LittleEndian(
                bytes.AsSpan(cursor, sizeof(ushort)));
            cursor += sizeof(ushort);
            var references = new List<SegmentRoutingReference>(
                referenceCount);

            for (var referenceIndex = 0;
                 referenceIndex < referenceCount;
                 referenceIndex++)
            {
                EnsureAvailable(bytes, cursor, 1, path);
                var segmentLength = bytes[cursor++];
                if (segmentLength == 0)
                {
                    throw new InvalidDataException(
                        $"Segmento vazio no índice de roteamento: {path}");
                }

                EnsureAvailable(
                    bytes,
                    cursor,
                    segmentLength + sizeof(ulong) + sizeof(uint),
                    path);
                var segmentId = StrictUtf8.GetString(
                    bytes,
                    cursor,
                    segmentLength);
                cursor += segmentLength;
                var offset = BinaryPrimitives.ReadUInt64LittleEndian(
                    bytes.AsSpan(cursor, sizeof(ulong)));
                cursor += sizeof(ulong);
                var length = BinaryPrimitives.ReadUInt32LittleEndian(
                    bytes.AsSpan(cursor, sizeof(uint)));
                cursor += sizeof(uint);
                references.Add(new SegmentRoutingReference(
                    segmentId,
                    offset,
                    length));
            }

            if (!routes.TryAdd(cnpj, references))
            {
                throw new InvalidDataException(
                    $"CNPJ duplicado no índice de roteamento: {cnpj}");
            }
        }

        if (cursor != bytes.Length)
        {
            throw new InvalidDataException(
                $"Índice de roteamento contém bytes excedentes: {path}");
        }

        return routes;
    }

    private static IEnumerable<(string Cnpj, ulong Offset, uint Length)>
        ReadShardIndex(string path)
    {
        var bytes = File.ReadAllBytes(path);
        if (bytes.Length < BinaryIndexedShardWriter.HeaderSize
            || !bytes
                .AsSpan(0, ShardIndexMagic.Length)
                .SequenceEqual(ShardIndexMagic))
        {
            throw new InvalidDataException(
                $"Índice binário de shard inválido: {path}");
        }

        var recordCount = BinaryPrimitives.ReadUInt32LittleEndian(
            bytes.AsSpan(4, sizeof(uint)));
        var expectedLength = BinaryIndexedShardWriter.HeaderSize
                             + checked((int)recordCount)
                             * BinaryIndexedShardWriter.EntrySize;
        if (bytes.Length != expectedLength)
        {
            throw new InvalidDataException(
                $"Tamanho inválido do índice binário de shard: {path}");
        }

        for (var index = 0; index < recordCount; index++)
        {
            var start = BinaryIndexedShardWriter.HeaderSize
                        + checked((int)index)
                        * BinaryIndexedShardWriter.EntrySize;
            var cnpj = Encoding.ASCII.GetString(
                bytes,
                start,
                CnpjLength);
            var offset = BinaryPrimitives.ReadUInt64LittleEndian(
                bytes.AsSpan(start + CnpjLength, sizeof(ulong)));
            var length = BinaryPrimitives.ReadUInt32LittleEndian(
                bytes.AsSpan(
                    start + CnpjLength + sizeof(ulong),
                    sizeof(uint)));
            yield return (cnpj, offset, length);
        }
    }

    private static void ValidateSegmentId(string segmentId)
    {
        if (string.IsNullOrWhiteSpace(segmentId))
            throw new ArgumentException("O identificador do segmento é obrigatório.");

        var bytes = StrictUtf8.GetByteCount(segmentId);
        if (bytes > byte.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(segmentId),
                "O identificador do segmento deve ocupar no máximo 255 bytes.");
        }
    }

    private static void EnsureAvailable(
        byte[] bytes,
        int cursor,
        int required,
        string path)
    {
        if (cursor < 0
            || required < 0
            || cursor > bytes.Length - required)
        {
            throw new InvalidDataException(
                $"Índice de roteamento truncado: {path}");
        }
    }
}
