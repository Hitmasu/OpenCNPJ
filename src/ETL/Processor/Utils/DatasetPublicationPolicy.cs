using System.Collections.Generic;

namespace CNPJExporter.Utils;

internal static class DatasetPublicationPolicy
{
    public const int NoNewDatasetExitCode = 10;

    public static bool TrySelectMonthToProcess(
        string? requestedMonth,
        IReadOnlyCollection<string> availableMonths,
        out string? selectedMonth,
        out string latestAvailableMonth)
    {
        latestAvailableMonth = GetLatestAvailableMonth(availableMonths);

        if (!string.IsNullOrWhiteSpace(requestedMonth))
        {
            var normalizedRequestedMonth = requestedMonth.Trim();
            if (availableMonths.Contains(normalizedRequestedMonth, StringComparer.Ordinal))
            {
                selectedMonth = normalizedRequestedMonth;
                return true;
            }

            throw new InvalidOperationException($"Mês {normalizedRequestedMonth} não encontrado no compartilhamento público.");
        }

        var currentMonth = GetCurrentMonth();
        if (availableMonths.Contains(currentMonth, StringComparer.Ordinal))
        {
            selectedMonth = currentMonth;
            return true;
        }

        selectedMonth = null;
        return false;
    }

    public static string GetLatestAvailableMonth(IReadOnlyCollection<string> availableMonths)
    {
        if (availableMonths.Count == 0)
            throw new InvalidOperationException("Nenhuma pasta mensal encontrada no compartilhamento público da Receita.");

        return availableMonths
            .OrderBy(x => x, StringComparer.Ordinal)
            .Last();
    }

    public static string GetCurrentMonth()
    {
        return DateTimeOffset.Now.ToString("yyyy-MM");
    }

    // last_updated é a data da publicação: um reprocessamento de competência anterior não pode
    // fazer a base do mês corrente parecer já publicada. Releases sem competência usam last_updated.
    public static bool TryGetPublishedMonth(string? sourceSnapshotMonth, string? lastUpdated, out string? publishedMonth)
    {
        if (DatasetPathResolver.IsDatasetKey(sourceSnapshotMonth))
        {
            publishedMonth = sourceSnapshotMonth;
            return true;
        }

        return TryGetPublishedMonth(lastUpdated, out publishedMonth);
    }

    public static bool TryGetPublishedMonth(string? lastUpdated, out string? publishedMonth)
    {
        publishedMonth = null;
        if (string.IsNullOrWhiteSpace(lastUpdated))
            return false;

        if (!DateTimeOffset.TryParse(lastUpdated, out var parsed))
            return false;

        publishedMonth = parsed.ToString("yyyy-MM");
        return true;
    }
}
