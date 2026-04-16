using CNPJExporter.Modules.Cno;
using CNPJExporter.Modules.Cno.Configuration;

namespace CNPJExporter.Integrations;

public static class DataIntegrationRegistry
{
    public static IReadOnlyList<IDataIntegration> CreateDefault()
    {
        var integrations = new List<IDataIntegration>();

        if (Configuration.AppConfig.Current.CnoIntegration.Enabled)
        {
            var cno = Configuration.AppConfig.Current.CnoIntegration;
            integrations.Add(new DataIntegration(new IntegrationOptions
            {
                Enabled = cno.Enabled,
                PublicShareRoot = cno.PublicShareRoot,
                ZipFileName = cno.ZipFileName,
                RefreshHours = cno.RefreshHours,
                ShardPrefixLength = Configuration.AppConfig.Current.Shards.PrefixLength
            }));
        }

        return integrations;
    }
}
