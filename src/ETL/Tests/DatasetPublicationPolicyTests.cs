using CNPJExporter.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class DatasetPublicationPolicyTests
{
    [TestMethod]
    public void TrySelectMonthToProcess_WithoutRequestedMonth_UsesCurrentMonthWhenAvailable()
    {
        var currentMonth = DatasetPublicationPolicy.GetCurrentMonth();
        var previousMonth = DateTimeOffset.Now.AddMonths(-1).ToString("yyyy-MM");
        var nextMonth = DateTimeOffset.Now.AddMonths(1).ToString("yyyy-MM");
        var result = DatasetPublicationPolicy.TrySelectMonthToProcess(null, [previousMonth, currentMonth, nextMonth], out var selectedMonth, out var latestAvailableMonth);

        Assert.IsTrue(result);
        Assert.AreEqual(currentMonth, selectedMonth);
        Assert.AreEqual(nextMonth, latestAvailableMonth);
    }

    [TestMethod]
    public void TrySelectMonthToProcess_WithRequestedMonth_UsesExactMonth()
    {
        var result = DatasetPublicationPolicy.TrySelectMonthToProcess("2026-03", ["2026-02", "2026-03"], out var selectedMonth, out var latestAvailableMonth);

        Assert.IsTrue(result);
        Assert.AreEqual("2026-03", selectedMonth);
        Assert.AreEqual("2026-03", latestAvailableMonth);
    }

    [TestMethod]
    public void TrySelectMonthToProcess_WithoutRequestedMonth_ReturnsFalseWhenCurrentMonthIsUnavailable()
    {
        var currentMonth = DatasetPublicationPolicy.GetCurrentMonth();
        var previousMonth = DateTimeOffset.Now.AddMonths(-1).ToString("yyyy-MM");
        var result = DatasetPublicationPolicy.TrySelectMonthToProcess(null, [previousMonth], out var selectedMonth, out var latestAvailableMonth);

        Assert.IsFalse(result);
        Assert.IsNull(selectedMonth);
        Assert.AreEqual(previousMonth, latestAvailableMonth);
        Assert.AreNotEqual(currentMonth, latestAvailableMonth);
    }

    [TestMethod]
    public void TryGetPublishedMonth_ReturnsMonthWhenTimestampIsValid()
    {
        var result = DatasetPublicationPolicy.TryGetPublishedMonth("2026-04-05T03:04:05Z", out var publishedMonth);

        Assert.IsTrue(result);
        Assert.AreEqual("2026-04", publishedMonth);
    }

    [TestMethod]
    public void TryGetPublishedMonth_ReturnsFalseWhenTimestampIsInvalid()
    {
        var result = DatasetPublicationPolicy.TryGetPublishedMonth("invalid", out var publishedMonth);

        Assert.IsFalse(result);
        Assert.IsNull(publishedMonth);
    }
}
