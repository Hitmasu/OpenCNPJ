using CNPJExporter.Modules.Receita.Processors;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class LgpdPolicyTests
{
    [TestMethod]
    public void Load_ShouldRejectUnknownField()
    {
        var path = WriteTempPolicy("""{ "11122233000181": ["razao_socal"] }""");

        try
        {
            Assert.ThrowsException<InvalidOperationException>(() => LgpdPolicy.Load(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [TestMethod]
    public void Load_ShouldRejectInvalidCnpj()
    {
        var path = WriteTempPolicy("""{ "123": ["email"] }""");

        try
        {
            Assert.ThrowsException<InvalidOperationException>(() => LgpdPolicy.Load(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [TestMethod]
    public void Load_ShouldFailWhenFileIsMissing()
    {
        var path = Path.Combine(Path.GetTempPath(), $"opencnpj-lgpd-missing-{Guid.NewGuid():N}.json");

        Assert.ThrowsException<FileNotFoundException>(() => LgpdPolicy.Load(path));
    }

    [TestMethod]
    public void LoadDefault_ShouldReadRepositoryPolicy()
    {
        LgpdPolicy.LoadDefault();
    }

    private static string WriteTempPolicy(string json)
    {
        var path = Path.Combine(Path.GetTempPath(), $"opencnpj-lgpd-{Guid.NewGuid():N}.json");
        File.WriteAllText(path, json);
        return path;
    }
}
