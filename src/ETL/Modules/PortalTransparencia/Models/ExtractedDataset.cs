namespace CNPJExporter.Modules.PortalTransparencia.Models;

internal sealed record ExtractedDataset(IReadOnlyList<string> CsvPaths)
{
    public static ExtractedDataset Combine(IEnumerable<ExtractedDataset> datasets) =>
        new(
            datasets
                .SelectMany(dataset => dataset.CsvPaths)
                .ToArray());

    public string RequireFileEnding(string suffix)
    {
        var matches = CsvPaths
            .Where(path => Path.GetFileName(path).EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        return matches.Length switch
        {
            1 => matches[0],
            0 => throw new InvalidOperationException(
                $"Arquivo obrigatório *{suffix} não foi encontrado no ZIP do Portal da Transparência."),
            _ => throw new InvalidOperationException(
                $"Mais de um arquivo *{suffix} foi encontrado no ZIP do Portal da Transparência.")
        };
    }

    public IReadOnlyList<string> RequireFilesEnding(string suffix)
    {
        var matches = CsvPaths
            .Where(path => Path.GetFileName(path).EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
            .OrderBy(path => Path.GetFileName(path), StringComparer.Ordinal)
            .ToArray();

        if (matches.Length == 0)
        {
            throw new InvalidOperationException(
                $"Arquivo obrigatório *{suffix} não foi encontrado nos ZIPs do Portal da Transparência.");
        }

        return matches;
    }
}
