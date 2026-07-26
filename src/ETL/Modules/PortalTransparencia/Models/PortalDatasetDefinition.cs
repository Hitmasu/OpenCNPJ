namespace CNPJExporter.Modules.PortalTransparencia.Models;

internal enum PortalDatasetPublicationMode
{
    Snapshot,
    SegmentedHistory
}

internal enum PortalArtifactPeriodicity
{
    Monthly,
    Annual,
    Singleton
}

internal sealed record PortalDatasetDefinition(
    string Key,
    string CatalogSlug,
    string ArchiveToken,
    TimeSpan RefreshInterval,
    IReadOnlyList<string> RequiredCsvSuffixes,
    PortalDatasetPublicationMode PublicationMode = PortalDatasetPublicationMode.Snapshot,
    PortalArtifactPeriodicity ArtifactPeriodicity = PortalArtifactPeriodicity.Monthly,
    string? SegmentCollectionProperty = null,
    IReadOnlySet<string>? KnownUnavailableDateTokens = null)
{
    public const int HistoricalMinimumYear = 2013;

    public bool IsSegmented =>
        PublicationMode == PortalDatasetPublicationMode.SegmentedHistory;

    public IReadOnlySet<string> EffectiveKnownUnavailableDateTokens =>
        KnownUnavailableDateTokens ?? new HashSet<string>(StringComparer.Ordinal);

    public static readonly IReadOnlyList<PortalDatasetDefinition> All =
    [
        new(
            Key: "favorecidos_pj",
            CatalogSlug: "favorecidos-pj",
            ArchiveToken: "FavorecidosPJ",
            RefreshInterval: TimeSpan.FromHours(24),
            RequiredCsvSuffixes:
            [
                "_CNPJ.csv",
                "_CNAE.csv",
                "_NaturezaJuridica.csv"
            ]),
        new(
            Key: "ceis",
            CatalogSlug: "ceis",
            ArchiveToken: "CEIS",
            RefreshInterval: TimeSpan.FromHours(4),
            RequiredCsvSuffixes: ["_CEIS.csv"]),
        new(
            Key: "cepim",
            CatalogSlug: "cepim",
            ArchiveToken: "CEPIM",
            RefreshInterval: TimeSpan.FromHours(24),
            RequiredCsvSuffixes: ["_CEPIM.csv"]),
        new(
            Key: "cnep",
            CatalogSlug: "cnep",
            ArchiveToken: "CNEP",
            RefreshInterval: TimeSpan.FromHours(4),
            RequiredCsvSuffixes: ["_CNEP.csv"]),
        new(
            Key: "acordos_leniencia",
            CatalogSlug: "acordos-leniencia",
            ArchiveToken: "AcordosLeniencia",
            RefreshInterval: TimeSpan.FromHours(4),
            RequiredCsvSuffixes:
            [
                "_Acordos.csv",
                "_Efeitos.csv"
            ]),
        new(
            Key: "licitacoes",
            CatalogSlug: "licitacoes",
            ArchiveToken: "Licitacoes",
            RefreshInterval: TimeSpan.FromHours(24),
            RequiredCsvSuffixes:
            [
                "_Licitação.csv",
                "_ItemLicitação.csv",
                "_ParticipantesLicitação.csv",
                "_EmpenhosRelacionados.csv"
            ],
            PublicationMode: PortalDatasetPublicationMode.SegmentedHistory,
            ArtifactPeriodicity: PortalArtifactPeriodicity.Monthly,
            SegmentCollectionProperty: "licitacoes",
            KnownUnavailableDateTokens: new HashSet<string>(
                ["201812"],
                StringComparer.Ordinal)),
        new(
            Key: "contratos",
            CatalogSlug: "compras",
            ArchiveToken: "Compras",
            RefreshInterval: TimeSpan.FromHours(24),
            RequiredCsvSuffixes:
            [
                "_Compras.csv",
                "_ItemCompra.csv",
                "_TermoAditivo.csv",
                "_Apostilamento.csv"
            ],
            PublicationMode: PortalDatasetPublicationMode.SegmentedHistory,
            ArtifactPeriodicity: PortalArtifactPeriodicity.Monthly,
            SegmentCollectionProperty: "contratos"),
        new(
            Key: "renuncias_fiscais",
            CatalogSlug: "renuncias",
            ArchiveToken: "RenunciasFiscais",
            RefreshInterval: TimeSpan.FromHours(24),
            RequiredCsvSuffixes:
            [
                "_RenúnciasFiscais.csv",
                "_EmpresasHabilitadas.csv",
                "_EmpresasImunesOuIsentas.csv",
                "_RenúnciasFiscaisPorBeneficiário.csv"
            ],
            PublicationMode: PortalDatasetPublicationMode.SegmentedHistory,
            ArtifactPeriodicity: PortalArtifactPeriodicity.Annual,
            SegmentCollectionProperty: "renuncias"),
        new(
            Key: "notas_fiscais",
            CatalogSlug: "notas-fiscais",
            ArchiveToken: "NFe",
            RefreshInterval: TimeSpan.FromHours(24),
            RequiredCsvSuffixes:
            [
                "_NFe_NotaFiscal.csv",
                "_NFe_NotaFiscalItem.csv",
                "_NFe_NotaFiscalEvento.csv"
            ],
            PublicationMode: PortalDatasetPublicationMode.SegmentedHistory,
            ArtifactPeriodicity: PortalArtifactPeriodicity.Monthly,
            SegmentCollectionProperty: "notas_fiscais"),
        new(
            Key: "convenios",
            CatalogSlug: "convenios",
            ArchiveToken: "Convenios",
            RefreshInterval: TimeSpan.FromHours(24),
            RequiredCsvSuffixes:
            [
                "_Convenios.csv",
                "_Convenios_OrdensBancarias.csv"
            ]),
        new(
            Key: "emendas_parlamentares",
            CatalogSlug: "emendas-parlamentares",
            ArchiveToken: "EmendasParlamentares",
            RefreshInterval: TimeSpan.FromHours(24),
            RequiredCsvSuffixes:
            [
                "EmendasParlamentares_PorFavorecido.csv"
            ],
            ArtifactPeriodicity: PortalArtifactPeriodicity.Singleton),
        new(
            Key: "emendas_documentos",
            CatalogSlug: "emendas-parlamentares-documentos",
            ArchiveToken: "EmendasParlamentaresPorDocumento",
            RefreshInterval: TimeSpan.FromHours(24),
            RequiredCsvSuffixes:
            [
                "_EmendasParlamentares_PorDocumento.csv"
            ],
            PublicationMode: PortalDatasetPublicationMode.SegmentedHistory,
            ArtifactPeriodicity: PortalArtifactPeriodicity.Annual,
            SegmentCollectionProperty: "documentos")
    ];

    public static IReadOnlyList<PortalDatasetDefinition> ResolveEnabled(
        IReadOnlyCollection<string>? configuredKeys)
    {
        if (configuredKeys is null || configuredKeys.Count == 0)
            return All;

        var definitionsByName = new Dictionary<string, PortalDatasetDefinition>(
            StringComparer.OrdinalIgnoreCase);
        foreach (var definition in All)
        {
            definitionsByName[definition.Key] = definition;
            definitionsByName[definition.CatalogSlug] = definition;
        }
        var selected = new List<PortalDatasetDefinition>(configuredKeys.Count);
        var selectedKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var configuredKey in configuredKeys)
        {
            var normalized = configuredKey?.Trim();
            if (string.IsNullOrWhiteSpace(normalized)
                || !definitionsByName.TryGetValue(normalized, out var definition))
            {
                throw new InvalidOperationException(
                    $"Dataset do Portal da Transparência desconhecido: {configuredKey ?? "<null>"}.");
            }

            if (selectedKeys.Add(definition.Key))
                selected.Add(definition);
        }

        return selected;
    }

    public static PortalDatasetDefinition GetRequired(string key) =>
        All.Single(definition => string.Equals(definition.Key, key, StringComparison.Ordinal));
}
