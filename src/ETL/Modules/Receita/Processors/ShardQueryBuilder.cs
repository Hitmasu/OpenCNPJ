namespace CNPJExporter.Modules.Receita.Processors;

public sealed class ShardQueryBuilder
{
    private readonly string _parquetDir;

    public ShardQueryBuilder(string parquetDir)
    {
        _parquetDir = parquetDir;
    }

    public bool HasPartitionData(string tableName, string prefix)
    {
        var partitionDir = Path.Combine(_parquetDir, tableName, $"cnpj_prefix={prefix}");
        return Directory.Exists(partitionDir)
               && Directory.EnumerateFiles(partitionDir, "*.parquet", SearchOption.TopDirectoryOnly).Any();
    }

    public List<string> GetExistingShardPrefixes()
    {
        var partitionedDir = Path.Combine(_parquetDir, "estabelecimento");
        if (!Directory.Exists(partitionedDir))
            return [];

        return Directory.EnumerateDirectories(partitionedDir, "cnpj_prefix=*", SearchOption.TopDirectoryOnly)
            .Select(Path.GetFileName)
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Select(static name => name!["cnpj_prefix=".Length..])
            .Where(static prefix => !string.IsNullOrWhiteSpace(prefix))
            .OrderBy(static prefix => prefix, StringComparer.Ordinal)
            .ToList();
    }

    public int GetShardCount() => GetExistingShardPrefixes().Count;

    public string? BuildJsonQueryForPrefix(string prefix, bool includeCnpjColumn, string jsonAlias)
    {
        if (!HasPartitionData("estabelecimento", prefix))
            return null;

        var prefixLiteral = Sql.EscapeLiteral(prefix);
        var prefixes = new[] { prefix };
        var estabelecimentoRelation = BuildPartitionedReadSql("estabelecimento", prefixes, allowEmpty: false);
        var empresaRelation = BuildPartitionedReadSql("empresa", prefixes, allowEmpty: false);
        var simplesRelation = BuildPartitionedReadSql("simples", prefixes, allowEmpty: true);
        var socioRelation = BuildPartitionedReadSql("socio", prefixes, allowEmpty: true);
        var selectCols = includeCnpjColumn
            ? "e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj, to_json(struct_pack(\n" + JsonProjection.Fields +
              $"\n)) as {jsonAlias}"
            : $"to_json(struct_pack(\n" + JsonProjection.Fields + $"\n)) as {jsonAlias}";

        return $@"WITH estabelecimento_data AS (
                SELECT * FROM {estabelecimentoRelation}
            ),
            empresa_data AS (
                SELECT * FROM {empresaRelation}
            ),
            simples_data AS (
                SELECT * FROM {simplesRelation}
            ),
            socio_data AS (
                SELECT * FROM {socioRelation}
            ),
            {QsaProjection.BuildCte("socios_data", "socio_data", prefixLiteral)}
            SELECT {selectCols}
            FROM estabelecimento_data e
            LEFT JOIN empresa_data emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN simples_data s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN socios_data sd ON e.cnpj_prefix = sd.cnpj_prefix AND e.cnpj_basico = sd.cnpj_basico
            WHERE e.cnpj_prefix = '{prefixLiteral}'";
    }

    public string BuildJsonQueryForPrefixBatch(IReadOnlyList<string> prefixes, bool includeCnpjColumn, string jsonAlias)
    {
        var estabelecimentoRelation = BuildPartitionedReadSql("estabelecimento", prefixes, allowEmpty: false);
        var empresaRelation = BuildPartitionedReadSql("empresa", prefixes, allowEmpty: false);
        var simplesRelation = BuildPartitionedReadSql("simples", prefixes, allowEmpty: true);
        var socioRelation = BuildPartitionedReadSql("socio", prefixes, allowEmpty: true);
        var selectCols = includeCnpjColumn
            ? "e.cnpj_prefix as shard_prefix, e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj, to_json(struct_pack(\n" +
              JsonProjection.Fields + $"\n)) as {jsonAlias}"
            : $"e.cnpj_prefix as shard_prefix, to_json(struct_pack(\n{JsonProjection.Fields}\n)) as {jsonAlias}";

        return $@"WITH batch_estabelecimentos AS (
                SELECT * FROM {estabelecimentoRelation}
            ),
            batch_empresas AS (
                SELECT * FROM {empresaRelation}
            ),
            batch_simples AS (
                SELECT * FROM {simplesRelation}
            ),
            {QsaProjection.BuildCte("batch_socios", socioRelation)}
            SELECT {selectCols}
            FROM batch_estabelecimentos e
            LEFT JOIN batch_empresas emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN batch_simples s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN batch_socios sd ON e.cnpj_prefix = sd.cnpj_prefix AND e.cnpj_basico = sd.cnpj_basico
            ORDER BY e.cnpj_prefix, cnpj";
    }

    public string BuildJsonQueryForCnpj(
        string prefix,
        string cnpjBasico,
        string cnpjOrdem,
        string cnpjDv,
        string jsonAlias)
    {
        var prefixLiteral = Sql.EscapeLiteral(prefix);
        var cnpjBasicoLiteral = Sql.EscapeLiteral(cnpjBasico);
        var cnpjOrdemLiteral = Sql.EscapeLiteral(cnpjOrdem);
        var cnpjDvLiteral = Sql.EscapeLiteral(cnpjDv);
        var prefixes = new[] { prefix };
        var estabelecimentoRelation = BuildPartitionedReadSql("estabelecimento", prefixes, allowEmpty: false);
        var empresaRelation = BuildPartitionedReadSql("empresa", prefixes, allowEmpty: false);
        var simplesRelation = BuildPartitionedReadSql("simples", prefixes, allowEmpty: true);
        var socioRelation = BuildPartitionedReadSql("socio", prefixes, allowEmpty: true);
        var selectCols = $"to_json(struct_pack(\n" + JsonProjection.Fields + $"\n)) as {jsonAlias}";

        return $@"WITH estabelecimento_data AS (
                SELECT * FROM {estabelecimentoRelation}
            ),
            empresa_data AS (
                SELECT * FROM {empresaRelation}
            ),
            simples_data AS (
                SELECT * FROM {simplesRelation}
            ),
            socio_data AS (
                SELECT * FROM {socioRelation}
            ),
            {QsaProjection.BuildCte("socios_data", "socio_data", prefixLiteral)}
            SELECT {selectCols}
            FROM estabelecimento_data e
            LEFT JOIN empresa_data emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN simples_data s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN socios_data sd ON e.cnpj_prefix = sd.cnpj_prefix AND e.cnpj_basico = sd.cnpj_basico
            WHERE e.cnpj_prefix = '{prefixLiteral}'
              AND e.cnpj_basico = '{cnpjBasicoLiteral}'
              AND e.cnpj_ordem = '{cnpjOrdemLiteral}'
              AND e.cnpj_dv = '{cnpjDvLiteral}'";
    }

    private IEnumerable<string> GetPartitionGlobPaths(string tableName, IReadOnlyList<string> prefixes)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var prefix in prefixes)
        {
            var partitionDir = Path.Combine(_parquetDir, tableName, $"cnpj_prefix={prefix}");
            if (!Directory.Exists(partitionDir))
                continue;

            var glob = Path.Combine(partitionDir, "*.parquet");
            if (seen.Add(glob))
                yield return glob;
        }
    }

    private string BuildPartitionedReadSql(string tableName, IReadOnlyList<string> prefixes, bool allowEmpty)
    {
        var globs = GetPartitionGlobPaths(tableName, prefixes).ToList();
        if (globs.Count == 0)
        {
            if (!allowEmpty)
                throw new InvalidOperationException($"Nenhuma partição Parquet encontrada para {tableName}.");

            return BuildEmptyShardTableSql(tableName);
        }

        var pathListSql = string.Join(", ", globs.Select(path => $"'{Sql.EscapeLiteral(path)}'"));
        return $"read_parquet([{pathListSql}], hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})";
    }

    private static string BuildEmptyShardTableSql(string tableName)
    {
        return tableName switch
        {
            "empresa" =>
                "(SELECT CAST(NULL AS VARCHAR) AS cnpj_basico, CAST(NULL AS VARCHAR) AS razao_social, CAST(NULL AS VARCHAR) AS natureza_juridica, CAST(NULL AS VARCHAR) AS qualificacao_responsavel, CAST(NULL AS VARCHAR) AS capital_social, CAST(NULL AS VARCHAR) AS porte_empresa, CAST(NULL AS VARCHAR) AS ente_federativo, CAST(NULL AS VARCHAR) AS cnpj_prefix WHERE FALSE)",
            "simples" =>
                "(SELECT CAST(NULL AS VARCHAR) AS cnpj_basico, CAST(NULL AS VARCHAR) AS opcao_simples, CAST(NULL AS VARCHAR) AS data_opcao_simples, CAST(NULL AS VARCHAR) AS data_exclusao_simples, CAST(NULL AS VARCHAR) AS opcao_mei, CAST(NULL AS VARCHAR) AS data_opcao_mei, CAST(NULL AS VARCHAR) AS data_exclusao_mei, CAST(NULL AS VARCHAR) AS cnpj_prefix WHERE FALSE)",
            "socio" =>
                "(SELECT CAST(NULL AS VARCHAR) AS cnpj_basico, CAST(NULL AS VARCHAR) AS identificador_socio, CAST(NULL AS VARCHAR) AS nome_socio, CAST(NULL AS VARCHAR) AS cnpj_cpf_socio, CAST(NULL AS VARCHAR) AS qualificacao_socio, CAST(NULL AS VARCHAR) AS data_entrada_sociedade, CAST(NULL AS VARCHAR) AS codigo_pais, CAST(NULL AS VARCHAR) AS representante_legal, CAST(NULL AS VARCHAR) AS nome_representante, CAST(NULL AS VARCHAR) AS qualificacao_representante, CAST(NULL AS VARCHAR) AS faixa_etaria, CAST(NULL AS VARCHAR) AS cnpj_prefix WHERE FALSE)",
            "estabelecimento" =>
                "(SELECT CAST(NULL AS VARCHAR) AS cnpj_basico, CAST(NULL AS VARCHAR) AS cnpj_ordem, CAST(NULL AS VARCHAR) AS cnpj_dv, CAST(NULL AS VARCHAR) AS identificador_matriz_filial, CAST(NULL AS VARCHAR) AS nome_fantasia, CAST(NULL AS VARCHAR) AS situacao_cadastral, CAST(NULL AS VARCHAR) AS data_situacao_cadastral, CAST(NULL AS VARCHAR) AS motivo_situacao_cadastral, CAST(NULL AS VARCHAR) AS nome_cidade_exterior, CAST(NULL AS VARCHAR) AS codigo_pais, CAST(NULL AS VARCHAR) AS data_inicio_atividade, CAST(NULL AS VARCHAR) AS cnae_principal, CAST(NULL AS VARCHAR) AS cnaes_secundarios, CAST(NULL AS VARCHAR) AS tipo_logradouro, CAST(NULL AS VARCHAR) AS logradouro, CAST(NULL AS VARCHAR) AS numero, CAST(NULL AS VARCHAR) AS complemento, CAST(NULL AS VARCHAR) AS bairro, CAST(NULL AS VARCHAR) AS cep, CAST(NULL AS VARCHAR) AS uf, CAST(NULL AS VARCHAR) AS codigo_municipio, CAST(NULL AS VARCHAR) AS ddd1, CAST(NULL AS VARCHAR) AS telefone1, CAST(NULL AS VARCHAR) AS ddd2, CAST(NULL AS VARCHAR) AS telefone2, CAST(NULL AS VARCHAR) AS ddd_fax, CAST(NULL AS VARCHAR) AS fax, CAST(NULL AS VARCHAR) AS correio_eletronico, CAST(NULL AS VARCHAR) AS situacao_especial, CAST(NULL AS VARCHAR) AS data_situacao_especial, CAST(NULL AS VARCHAR) AS cnpj_prefix WHERE FALSE)",
            _ => throw new InvalidOperationException($"Tabela shard não suportada: {tableName}")
        };
    }
}
