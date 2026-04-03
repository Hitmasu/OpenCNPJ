using CNPJExporter.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class CnpjUtilsTests
{
    [TestMethod]
    public void IsValidFormat_ShouldAccept_AlphanumericCnpj()
    {
        Assert.IsTrue(CnpjUtils.IsValidFormat("12ABC34501DE35"));
        Assert.IsTrue(CnpjUtils.IsValidFormat("12.ABC.345/01DE-35"));
    }

    [TestMethod]
    public void ParseCnpj_ShouldPreserve_AlphanumericSections()
    {
        var (basico, ordem, dv) = CnpjUtils.ParseCnpj("12.ABC.345/01DE-35");

        Assert.AreEqual("12ABC345", basico);
        Assert.AreEqual("01DE", ordem);
        Assert.AreEqual("35", dv);
    }
}
