namespace CNPJExporter.Modules.Receita.Processors;

internal static class TableSchemas
{
    public static IReadOnlyDictionary<string, (string Pattern, string[] Columns)> CsvTables { get; } =
        new Dictionary<string, (string Pattern, string[] Columns)>
        {
            ["empresa"] = ("*EMPRECSV*", [
                "cnpj_basico", "razao_social", "natureza_juridica",
                "qualificacao_responsavel", "capital_social", "porte_empresa", "ente_federativo"
            ]),
            ["estabelecimento"] = ("*ESTABELE*", [
                "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
                "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
                "motivo_situacao_cadastral", "nome_cidade_exterior", "codigo_pais",
                "data_inicio_atividade", "cnae_principal", "cnaes_secundarios",
                "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
                "cep", "uf", "codigo_municipio", "ddd1", "telefone1", "ddd2",
                "telefone2", "ddd_fax", "fax", "correio_eletronico", "situacao_especial",
                "data_situacao_especial"
            ]),
            ["socio"] = ("*SOCIOCSV*", [
                "cnpj_basico", "identificador_socio", "nome_socio", "cnpj_cpf_socio",
                "qualificacao_socio", "data_entrada_sociedade", "codigo_pais",
                "representante_legal", "nome_representante", "qualificacao_representante",
                "faixa_etaria"
            ]),
            ["simples"] = ("*SIMPLES*", [
                "cnpj_basico", "opcao_simples", "data_opcao_simples",
                "data_exclusao_simples", "opcao_mei", "data_opcao_mei",
                "data_exclusao_mei"
            ]),
            ["cnae"] = ("*CNAECSV*", ["codigo", "descricao"]),
            ["motivo"] = ("*MOTICSV*", ["codigo", "descricao"]),
            ["municipio"] = ("*MUNICCSV*", ["codigo", "descricao"]),
            ["natureza"] = ("*NATJUCSV*", ["codigo", "descricao"]),
            ["pais"] = ("*PAISCSV*", ["codigo", "descricao"]),
            ["qualificacao"] = ("*QUALSCSV*", ["codigo", "descricao"])
        };

    public static IReadOnlySet<string> PartitionedTables { get; } =
        new HashSet<string>(StringComparer.Ordinal)
        {
            "estabelecimento",
            "empresa",
            "simples",
            "socio"
        };

    public static IReadOnlyDictionary<string, string> AuxiliaryTableGlobs { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["cnae"] = "cnae.parquet",
            ["motivo"] = "motivo.parquet",
            ["municipio"] = "municipio.parquet",
            ["natureza"] = "natureza.parquet",
            ["pais"] = "pais.parquet",
            ["qualificacao"] = "qualificacao.parquet"
        };
}
