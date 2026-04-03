using CNPJExporter.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class DirectoryUtilsTests
{
    [TestMethod]
    public async Task RecreateDirectoryAsync_ShouldReplace_NestedDirectoryTree()
    {
        var root = Path.Combine(Path.GetTempPath(), $"opencnpj-dirutils-{Guid.NewGuid():N}");
        var target = Path.Combine(root, "target");
        var nested = Path.Combine(target, "child", "grandchild");

        try
        {
            Directory.CreateDirectory(nested);
            await File.WriteAllTextAsync(Path.Combine(target, "root.txt"), "root");
            await File.WriteAllTextAsync(Path.Combine(nested, "deep.txt"), "deep");

            await DirectoryUtils.RecreateDirectoryAsync(target);

            Assert.IsTrue(Directory.Exists(target), "O diretório alvo deveria continuar existindo.");
            Assert.IsFalse(Directory.EnumerateFileSystemEntries(target).Any(), "O diretório recriado deveria estar vazio.");

            var leftovers = Directory.Exists(root)
                ? Directory.EnumerateDirectories(root, ".recreate-*", SearchOption.TopDirectoryOnly).ToList()
                : [];
            Assert.AreEqual(0, leftovers.Count, "Não deveria sobrar backup temporário após a recriação.");
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, true);
        }
    }
}
