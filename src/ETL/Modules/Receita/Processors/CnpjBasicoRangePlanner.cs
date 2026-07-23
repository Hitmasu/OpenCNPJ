namespace CNPJExporter.Modules.Receita.Processors;

public static class CnpjBasicoRangePlanner
{
    private const int CnpjBasicoLength = 8;
    private const int Radix = 36;
    private const string Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static readonly ulong MaxExclusive = PowRadix(CnpjBasicoLength);

    public static bool TryBuildFullRange(string prefix, out CnpjBasicoRange range)
    {
        range = default;

        if (string.IsNullOrWhiteSpace(prefix) || prefix.Length > CnpjBasicoLength)
            return false;

        ulong prefixValue = 0;
        foreach (var character in prefix.ToUpperInvariant())
        {
            var digit = Alphabet.IndexOf(character);
            if (digit < 0)
                return false;

            prefixValue = (prefixValue * Radix) + (ulong)digit;
        }

        var multiplier = PowRadix(CnpjBasicoLength - prefix.Length);
        var startInclusive = prefixValue * multiplier;
        range = new CnpjBasicoRange(startInclusive, startInclusive + multiplier);
        return true;
    }

    public static IReadOnlyList<CnpjBasicoRange> Split(CnpjBasicoRange range, int fanOut)
    {
        var width = range.EndExclusive - range.StartInclusive;
        if (width <= 1)
            return [range];

        var divisor = (ulong)Math.Max(2, fanOut);
        var chunkWidth = Math.Max(1UL, (width + divisor - 1) / divisor);
        var ranges = new List<CnpjBasicoRange>();

        for (var start = range.StartInclusive; start < range.EndExclusive; start += chunkWidth)
        {
            var end = Math.Min(range.EndExclusive, start + chunkWidth);
            ranges.Add(new CnpjBasicoRange(start, end));
        }

        return ranges;
    }

    internal static string ToLiteral(ulong value)
    {
        if (value >= MaxExclusive)
            throw new ArgumentOutOfRangeException(nameof(value));

        Span<char> characters = stackalloc char[CnpjBasicoLength];
        for (var index = CnpjBasicoLength - 1; index >= 0; index--)
        {
            characters[index] = Alphabet[(int)(value % Radix)];
            value /= Radix;
        }

        return new string(characters);
    }

    internal static bool IsMaximum(ulong value) => value >= MaxExclusive;

    private static ulong PowRadix(int exponent)
    {
        ulong result = 1;
        for (var index = 0; index < exponent; index++)
            result *= Radix;

        return result;
    }
}

public readonly record struct CnpjBasicoRange(ulong StartInclusive, ulong EndExclusive)
{
    public string StartLiteral => CnpjBasicoRangePlanner.ToLiteral(StartInclusive);

    public string? EndLiteral => CnpjBasicoRangePlanner.IsMaximum(EndExclusive)
        ? null
        : CnpjBasicoRangePlanner.ToLiteral(EndExclusive);

    public bool CanSplit => EndExclusive - StartInclusive > 1;

    public override string ToString() => EndLiteral is null
        ? $"[{StartLiteral}, max]"
        : $"[{StartLiteral}, {EndLiteral})";
}
