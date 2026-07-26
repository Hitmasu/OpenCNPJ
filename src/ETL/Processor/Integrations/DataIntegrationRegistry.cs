using CnoDataIntegration = CNPJExporter.Modules.Cno.DataIntegration;
using CnoIntegrationOptions = CNPJExporter.Modules.Cno.Configuration.IntegrationOptions;
using RntrcDataIntegration = CNPJExporter.Modules.Rntrc.DataIntegration;
using RntrcIntegrationOptions = CNPJExporter.Modules.Rntrc.Configuration.IntegrationOptions;
using PortalTransparenciaDataIntegration = CNPJExporter.Modules.PortalTransparencia.DataIntegration;
using PortalTransparenciaIntegrationOptions = CNPJExporter.Modules.PortalTransparencia.Configuration.IntegrationOptions;

namespace CNPJExporter.Integrations;

public static class DataIntegrationRegistry
{
    public static IReadOnlyList<IDataIntegration> CreateDefault()
    {
        var integrations = new List<IDataIntegration>();

        if (Configuration.AppConfig.Current.CnoIntegration.Enabled)
        {
            var cno = Configuration.AppConfig.Current.CnoIntegration;
            integrations.Add(new CnoDataIntegration(new CnoIntegrationOptions
            {
                Enabled = cno.Enabled,
                PublicShareRoot = cno.PublicShareRoot,
                ZipFileName = cno.ZipFileName,
                RefreshHours = cno.RefreshHours,
                ShardPrefixLength = Configuration.AppConfig.Current.Shards.PrefixLength
            }));
        }

        if (Configuration.AppConfig.Current.RntrcIntegration.Enabled)
        {
            var rntrc = Configuration.AppConfig.Current.RntrcIntegration;
            integrations.Add(new RntrcDataIntegration(new RntrcIntegrationOptions
            {
                Enabled = rntrc.Enabled,
                PackageShowUrl = rntrc.PackageShowUrl,
                RefreshHours = rntrc.RefreshHours,
                ShardPrefixLength = Configuration.AppConfig.Current.Shards.PrefixLength
            }));
        }

        if (Configuration.AppConfig.Current.PortalTransparenciaIntegration.Enabled)
        {
            var portal = Configuration.AppConfig.Current.PortalTransparenciaIntegration;
            integrations.AddRange(
                PortalTransparenciaDataIntegration.CreateEnabled(
                    new PortalTransparenciaIntegrationOptions
                    {
                        Enabled = portal.Enabled,
                        CatalogBaseUrl = portal.CatalogBaseUrl,
                        EnabledDatasets = portal.EnabledDatasets,
                        ShardPrefixLength = Configuration.AppConfig.Current.Shards.PrefixLength,
                        DuckDbThreads = portal.DuckDbThreads,
                        DuckDbMemoryLimit = portal.DuckDbMemoryLimit,
                        DuckDbMaxTempDirectorySize = portal.DuckDbMaxTempDirectorySize,
                        ProcessingPartitions = portal.ProcessingPartitions
                    }));
        }

        return integrations;
    }
}
