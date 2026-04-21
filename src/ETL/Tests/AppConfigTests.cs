using CNPJExporter.Configuration;
using CnoIntegrationOptions = CNPJExporter.Modules.Cno.Configuration.IntegrationOptions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RntrcIntegrationOptions = CNPJExporter.Modules.Rntrc.Configuration.IntegrationOptions;

namespace ETL.Tests;

[TestClass]
public sealed class AppConfigTests
{
    [TestMethod]
    public void ModuleSourceUrls_ShouldNotBeHardCodedOutsideConfigJson()
    {
        Assert.AreEqual(string.Empty, new AppConfig.CnoIntegrationSettings().PublicShareRoot);
        Assert.AreEqual(string.Empty, new AppConfig.RntrcIntegrationSettings().PackageShowUrl);
        Assert.AreEqual(string.Empty, new CnoIntegrationOptions().PublicShareRoot);
        Assert.AreEqual(string.Empty, new RntrcIntegrationOptions().PackageShowUrl);
    }
}
