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
	            cnae_lookup AS (
	                SELECT map_from_entries(array_agg(struct_pack(key := codigo, value := descricao))) AS descricoes
	                FROM cnae
	            ),
	            {BuildQsaCte("socios_data", prefixes, prefixLiteral: prefixLiteral)}
	            SELECT {selectCols}
	            FROM estabelecimento_data e
	            CROSS JOIN cnae_lookup
	            LEFT JOIN empresa_data emp ON e.cnpj_basico = emp.cnpj_basico
	            LEFT JOIN simples_data s ON e.cnpj_basico = s.cnpj_basico
	            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
	            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
	            LEFT JOIN motivo mot ON e.motivo_situacao_cadastral = mot.codigo
	            LEFT JOIN pais pais_est ON e.codigo_pais = pais_est.codigo
	            LEFT JOIN qualificacao qr ON emp.qualificacao_responsavel = qr.codigo
	            LEFT JOIN socios_data sd ON e.cnpj_prefix = sd.cnpj_prefix AND e.cnpj_basico = sd.cnpj_basico
	            WHERE e.cnpj_prefix = '{prefixLiteral}'";
    }

    public string BuildJsonQueryForPrefixBatch(
        IReadOnlyList<string> prefixes,
        bool includeCnpjColumn,
        string jsonAlias,
        string? cnpjBasicoStartInclusive = null,
        string? cnpjBasicoEndExclusive = null)
    {
        var cnpjBasicoStartLiteral = string.IsNullOrWhiteSpace(cnpjBasicoStartInclusive)
            ? null
            : Sql.EscapeLiteral(cnpjBasicoStartInclusive);
        var cnpjBasicoEndLiteral = string.IsNullOrWhiteSpace(cnpjBasicoEndExclusive)
            ? null
            : Sql.EscapeLiteral(cnpjBasicoEndExclusive);
        var cnpjBasicoWhere = BuildCnpjBasicoWhereClause(cnpjBasicoStartLiteral, cnpjBasicoEndLiteral);
        var estabelecimentoRelation = BuildPartitionedReadSql("estabelecimento", prefixes, allowEmpty: false);
        var empresaRelation = BuildPartitionedReadSql("empresa", prefixes, allowEmpty: false);
        var simplesRelation = BuildPartitionedReadSql("simples", prefixes, allowEmpty: true);
        var selectCols = includeCnpjColumn
            ? "e.cnpj_prefix as shard_prefix, e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj, to_json(struct_pack(\n" +
              JsonProjection.Fields + $"\n)) as {jsonAlias}"
            : $"e.cnpj_prefix as shard_prefix, to_json(struct_pack(\n{JsonProjection.Fields}\n)) as {jsonAlias}";

        return $@"WITH batch_estabelecimentos AS (
                SELECT * FROM {estabelecimentoRelation}{cnpjBasicoWhere}
            ),
            batch_empresas AS (
                SELECT * FROM {empresaRelation}{cnpjBasicoWhere}
            ),
	            batch_simples AS (
	                SELECT * FROM {simplesRelation}{cnpjBasicoWhere}
	            ),
	            cnae_lookup AS (
	                SELECT map_from_entries(array_agg(struct_pack(key := codigo, value := descricao))) AS descricoes
	                FROM cnae
	            ),
	            {BuildQsaCte("batch_socios", prefixes, cnpjBasicoStartInclusive: cnpjBasicoStartLiteral, cnpjBasicoEndExclusive: cnpjBasicoEndLiteral)}
	            SELECT {selectCols}
	            FROM batch_estabelecimentos e
	            CROSS JOIN cnae_lookup
	            LEFT JOIN batch_empresas emp ON e.cnpj_basico = emp.cnpj_basico
	            LEFT JOIN batch_simples s ON e.cnpj_basico = s.cnpj_basico
	            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
	            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
	            LEFT JOIN motivo mot ON e.motivo_situacao_cadastral = mot.codigo
	            LEFT JOIN pais pais_est ON e.codigo_pais = pais_est.codigo
	            LEFT JOIN qualificacao qr ON emp.qualificacao_responsavel = qr.codigo
	            LEFT JOIN batch_socios sd ON e.cnpj_prefix = sd.cnpj_prefix AND e.cnpj_basico = sd.cnpj_basico";
    }

    public string BuildColumnarQueryForPrefixBatch(IReadOnlyList<string> prefixes)
    {
        var estabelecimentoRelation = BuildPartitionedReadSql("estabelecimento", prefixes, allowEmpty: false);
        var empresaRelation = BuildPartitionedReadSql("empresa", prefixes, allowEmpty: false);
        var simplesRelation = BuildPartitionedReadSql("simples", prefixes, allowEmpty: true);

        return $@"WITH batch_estabelecimentos AS (
                SELECT * FROM {estabelecimentoRelation}
            ),
            batch_empresas AS (
                SELECT * FROM {empresaRelation}
            ),
	            batch_simples AS (
	                SELECT * FROM {simplesRelation}
	            ),
	            cnae_lookup AS (
	                SELECT map_from_entries(array_agg(struct_pack(key := codigo, value := descricao))) AS descricoes
	                FROM cnae
	            ),
	            {BuildQsaCte("batch_socios", prefixes)}
	            SELECT
                    e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
                    TRY_CAST(e.cnpj_prefix AS INTEGER) AS cnpj_prefix,
                    COALESCE(emp.razao_social, '') AS razao_social,
                    COALESCE(e.nome_fantasia, '') AS nome_fantasia,
                    CASE LPAD(e.situacao_cadastral, 2, '0')
                        WHEN '01' THEN 'Nula'
                        WHEN '02' THEN 'Ativa'
                        WHEN '03' THEN 'Suspensa'
                        WHEN '04' THEN 'Inapta'
                        WHEN '08' THEN 'Baixada'
                        ELSE COALESCE(e.situacao_cadastral, '')
                    END AS situacao_cadastral,
                    CASE
                        WHEN e.data_situacao_cadastral ~ '^[0-9]{{8}}$'
                        THEN TRY_CAST(SUBSTRING(e.data_situacao_cadastral, 1, 4) || '-' ||
                                      SUBSTRING(e.data_situacao_cadastral, 5, 2) || '-' ||
                                      SUBSTRING(e.data_situacao_cadastral, 7, 2) AS DATE)
                        ELSE TRY_CAST(NULLIF(e.data_situacao_cadastral, '') AS DATE)
                    END AS data_situacao_cadastral,
                    CASE e.identificador_matriz_filial
                        WHEN '1' THEN 'Matriz'
                        WHEN '2' THEN 'Filial'
                        ELSE COALESCE(e.identificador_matriz_filial, '')
                    END AS matriz_filial,
                    CASE
                        WHEN e.data_inicio_atividade ~ '^[0-9]{{8}}$'
                        THEN TRY_CAST(SUBSTRING(e.data_inicio_atividade, 1, 4) || '-' ||
                                      SUBSTRING(e.data_inicio_atividade, 5, 2) || '-' ||
                                      SUBSTRING(e.data_inicio_atividade, 7, 2) AS DATE)
                        ELSE TRY_CAST(NULLIF(e.data_inicio_atividade, '') AS DATE)
                    END AS data_inicio_atividade,
                    COALESCE(e.cnae_principal, '') AS cnae_principal,
                    CASE
                        WHEN e.cnaes_secundarios IS NOT NULL AND e.cnaes_secundarios != ''
                        THEN string_split(e.cnaes_secundarios, ',')
                        ELSE []
                    END AS cnaes_secundarios,
                    list_transform(
                        list_filter(
                            list_concat(
                                [COALESCE(e.cnae_principal, '')],
                                CASE
                                    WHEN e.cnaes_secundarios IS NOT NULL AND e.cnaes_secundarios != ''
                                    THEN string_split(e.cnaes_secundarios, ',')
                                    ELSE []
                                END),
                            codigo -> codigo != ''),
                        codigo -> struct_pack(
                            codigo := codigo,
                            descricao := COALESCE(map_extract_value(cnae_lookup.descricoes, codigo), ''),
                            is_principal := codigo = COALESCE(e.cnae_principal, '')
                        )) AS cnaes,
                    COALESCE(nat.descricao, '') AS natureza_juridica,
                    COALESCE(e.tipo_logradouro, '') AS tipo_logradouro,
                    COALESCE(e.logradouro, '') AS logradouro,
                    COALESCE(e.numero, '') AS numero,
                    COALESCE(e.complemento, '') AS complemento,
                    COALESCE(e.bairro, '') AS bairro,
                    COALESCE(e.cep, '') AS cep,
                    COALESCE(e.uf, '') AS uf,
                    COALESCE(mun.descricao, '') AS municipio,
                    COALESCE(e.codigo_municipio, '') AS codigo_municipio,
                    COALESCE(e.correio_eletronico, '') AS email,
                    list_filter([
                        CASE WHEN e.ddd1 IS NOT NULL OR e.telefone1 IS NOT NULL
                             THEN struct_pack(ddd := COALESCE(e.ddd1, ''), numero := COALESCE(e.telefone1, ''), is_fax := false)
                             ELSE NULL
                        END,
                        CASE WHEN e.ddd2 IS NOT NULL OR e.telefone2 IS NOT NULL
                             THEN struct_pack(ddd := COALESCE(e.ddd2, ''), numero := COALESCE(e.telefone2, ''), is_fax := false)
                             ELSE NULL
                        END,
                        CASE WHEN e.ddd_fax IS NOT NULL OR e.fax IS NOT NULL
                             THEN struct_pack(ddd := COALESCE(e.ddd_fax, ''), numero := COALESCE(e.fax, ''), is_fax := true)
                             ELSE NULL
                        END
                    ], telefone -> telefone IS NOT NULL) AS telefones,
                    TRY_CAST(REPLACE(NULLIF(emp.capital_social, ''), ',', '.') AS DECIMAL(18, 2)) AS capital_social,
                    struct_pack(
                        codigo := COALESCE(emp.qualificacao_responsavel, ''),
                        descricao := COALESCE(qr.descricao, '')
                    ) AS qualificacao_responsavel,
                    COALESCE(emp.ente_federativo, '') AS ente_federativo,
                    CASE emp.porte_empresa
                        WHEN '00' THEN 'Não informado'
                        WHEN '01' THEN 'Microempresa (ME)'
                        WHEN '03' THEN 'Empresa de Pequeno Porte (EPP)'
                        WHEN '05' THEN 'Demais'
                        ELSE COALESCE(emp.porte_empresa, '')
                    END AS porte_empresa,
                    CASE
                        WHEN UPPER(COALESCE(s.opcao_simples, '')) = 'S' THEN true
                        WHEN UPPER(COALESCE(s.opcao_simples, '')) = 'N' THEN false
                        ELSE NULL
                    END AS opcao_simples,
                    CASE
                        WHEN s.data_opcao_simples ~ '^[0-9]{{8}}$'
                        THEN TRY_CAST(SUBSTRING(s.data_opcao_simples, 1, 4) || '-' ||
                                      SUBSTRING(s.data_opcao_simples, 5, 2) || '-' ||
                                      SUBSTRING(s.data_opcao_simples, 7, 2) AS DATE)
                        ELSE TRY_CAST(NULLIF(s.data_opcao_simples, '') AS DATE)
                    END AS data_opcao_simples,
                    CASE
                        WHEN s.data_exclusao_simples ~ '^[0-9]{{8}}$'
                        THEN TRY_CAST(SUBSTRING(s.data_exclusao_simples, 1, 4) || '-' ||
                                      SUBSTRING(s.data_exclusao_simples, 5, 2) || '-' ||
                                      SUBSTRING(s.data_exclusao_simples, 7, 2) AS DATE)
                        ELSE TRY_CAST(NULLIF(s.data_exclusao_simples, '') AS DATE)
                    END AS data_exclusao_simples,
                    CASE
                        WHEN UPPER(COALESCE(s.opcao_mei, '')) = 'S' THEN true
                        WHEN UPPER(COALESCE(s.opcao_mei, '')) = 'N' THEN false
                        ELSE NULL
                    END AS opcao_mei,
                    CASE
                        WHEN s.data_opcao_mei ~ '^[0-9]{{8}}$'
                        THEN TRY_CAST(SUBSTRING(s.data_opcao_mei, 1, 4) || '-' ||
                                      SUBSTRING(s.data_opcao_mei, 5, 2) || '-' ||
                                      SUBSTRING(s.data_opcao_mei, 7, 2) AS DATE)
                        ELSE TRY_CAST(NULLIF(s.data_opcao_mei, '') AS DATE)
                    END AS data_opcao_mei,
                    CASE
                        WHEN s.data_exclusao_mei ~ '^[0-9]{{8}}$'
                        THEN TRY_CAST(SUBSTRING(s.data_exclusao_mei, 1, 4) || '-' ||
                                      SUBSTRING(s.data_exclusao_mei, 5, 2) || '-' ||
                                      SUBSTRING(s.data_exclusao_mei, 7, 2) AS DATE)
                        ELSE TRY_CAST(NULLIF(s.data_exclusao_mei, '') AS DATE)
                    END AS data_exclusao_mei,
                    struct_pack(
                        codigo := COALESCE(e.motivo_situacao_cadastral, ''),
                        descricao := COALESCE(mot.descricao, '')
                    ) AS motivo_situacao_cadastral,
                    COALESCE(e.nome_cidade_exterior, '') AS nome_cidade_exterior,
                    COALESCE(e.codigo_pais, '') AS codigo_pais,
                    struct_pack(
                        codigo := COALESCE(e.codigo_pais, ''),
                        descricao := COALESCE(pais_est.descricao, '')
                    ) AS pais,
                    COALESCE(e.situacao_especial, '') AS situacao_especial,
                    CASE
                        WHEN e.data_situacao_especial ~ '^[0-9]{{8}}$'
                        THEN TRY_CAST(SUBSTRING(e.data_situacao_especial, 1, 4) || '-' ||
                                      SUBSTRING(e.data_situacao_especial, 5, 2) || '-' ||
                                      SUBSTRING(e.data_situacao_especial, 7, 2) AS DATE)
                        ELSE TRY_CAST(NULLIF(e.data_situacao_especial, '') AS DATE)
                    END AS data_situacao_especial,
                    COALESCE(
                        list_transform(sd.qsa_data, socio -> struct_pack(
                            nome_socio := socio.nome_socio,
                            cnpj_cpf_socio := socio.cnpj_cpf_socio,
                            qualificacao_socio := socio.qualificacao_socio,
                            data_entrada_sociedade := TRY_CAST(NULLIF(socio.data_entrada_sociedade, '') AS DATE),
                            identificador_socio := socio.identificador_socio,
                            codigo_pais := socio.codigo_pais,
                            pais := socio.pais,
                            representante_legal := socio.representante_legal,
                            nome_representante := socio.nome_representante,
                            qualificacao_representante := socio.qualificacao_representante,
                            faixa_etaria := socio.faixa_etaria
                        )),
                        []
                    ) AS QSA
	            FROM batch_estabelecimentos e
	            CROSS JOIN cnae_lookup
	            LEFT JOIN batch_empresas emp ON e.cnpj_basico = emp.cnpj_basico
	            LEFT JOIN batch_simples s ON e.cnpj_basico = s.cnpj_basico
	            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
	            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
	            LEFT JOIN motivo mot ON e.motivo_situacao_cadastral = mot.codigo
	            LEFT JOIN pais pais_est ON e.codigo_pais = pais_est.codigo
	            LEFT JOIN qualificacao qr ON emp.qualificacao_responsavel = qr.codigo
	            LEFT JOIN batch_socios sd ON e.cnpj_prefix = sd.cnpj_prefix AND e.cnpj_basico = sd.cnpj_basico";
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
	            cnae_lookup AS (
	                SELECT map_from_entries(array_agg(struct_pack(key := codigo, value := descricao))) AS descricoes
	                FROM cnae
	            ),
	            {BuildQsaCte("socios_data", prefixes, prefixLiteral, cnpjBasicoLiteral)}
	            SELECT {selectCols}
	            FROM estabelecimento_data e
	            CROSS JOIN cnae_lookup
	            LEFT JOIN empresa_data emp ON e.cnpj_basico = emp.cnpj_basico
	            LEFT JOIN simples_data s ON e.cnpj_basico = s.cnpj_basico
	            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
	            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
	            LEFT JOIN motivo mot ON e.motivo_situacao_cadastral = mot.codigo
	            LEFT JOIN pais pais_est ON e.codigo_pais = pais_est.codigo
	            LEFT JOIN qualificacao qr ON emp.qualificacao_responsavel = qr.codigo
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

    private string? TryBuildPartitionedReadSql(string tableName, IReadOnlyList<string> prefixes)
    {
        var globs = GetPartitionGlobPaths(tableName, prefixes).ToList();
        return globs.Count == 0
            ? null
            : BuildReadParquetSql(globs);
    }

    private string BuildPartitionedReadSql(string tableName, IReadOnlyList<string> prefixes, bool allowEmpty)
    {
        var readSql = TryBuildPartitionedReadSql(tableName, prefixes);
        if (readSql is not null)
            return readSql;

        if (!allowEmpty)
            throw new InvalidOperationException($"Nenhuma partição Parquet encontrada para {tableName}.");

        return BuildEmptyShardTableSql(tableName);
    }

    private static string BuildReadParquetSql(IReadOnlyList<string> globs)
    {
        var pathListSql = string.Join(", ", globs.Select(path => $"'{Sql.EscapeLiteral(path)}'"));
        return $"read_parquet([{pathListSql}], hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})";
    }

    private string BuildQsaCte(
        string cteName,
        IReadOnlyList<string> prefixes,
        string? prefixLiteral = null,
        string? cnpjBasicoLiteral = null,
        string? cnpjBasicoStartInclusive = null,
        string? cnpjBasicoEndExclusive = null)
    {
        var materializedQsaRelation = TryBuildPartitionedReadSql("qsa", prefixes);
        if (materializedQsaRelation is not null)
        {
            var where = BuildQsaMaterializedWhereClause(
                prefixLiteral,
                cnpjBasicoLiteral,
                cnpjBasicoStartInclusive,
                cnpjBasicoEndExclusive);

            return $@"{cteName} AS (
                SELECT cnpj_prefix, cnpj_basico, qsa_data
                FROM {materializedQsaRelation}{where}
            )";
        }

        var socioRelation = BuildPartitionedReadSql("socio", prefixes, allowEmpty: true);
        return QsaProjection.BuildCte(
            cteName,
            socioRelation,
            prefixLiteral,
            cnpjBasicoLiteral,
            cnpjBasicoStartInclusive,
            cnpjBasicoEndExclusive);
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

    private static string BuildCnpjBasicoWhereClause(string? startInclusiveLiteral, string? endExclusiveLiteral)
    {
        var predicates = new List<string>();
        if (!string.IsNullOrWhiteSpace(startInclusiveLiteral))
            predicates.Add($"cnpj_basico >= '{startInclusiveLiteral}'");
        if (!string.IsNullOrWhiteSpace(endExclusiveLiteral))
            predicates.Add($"cnpj_basico < '{endExclusiveLiteral}'");

        return predicates.Count == 0
            ? string.Empty
            : $" WHERE {string.Join(" AND ", predicates)}";
    }

    private static string BuildQsaMaterializedWhereClause(
        string? prefixLiteral,
        string? cnpjBasicoLiteral,
        string? startInclusiveLiteral,
        string? endExclusiveLiteral)
    {
        var predicates = new List<string>();
        if (!string.IsNullOrWhiteSpace(prefixLiteral))
            predicates.Add($"cnpj_prefix = '{prefixLiteral}'");
        if (!string.IsNullOrWhiteSpace(cnpjBasicoLiteral))
            predicates.Add($"cnpj_basico = '{cnpjBasicoLiteral}'");
        if (!string.IsNullOrWhiteSpace(startInclusiveLiteral))
            predicates.Add($"cnpj_basico >= '{startInclusiveLiteral}'");
        if (!string.IsNullOrWhiteSpace(endExclusiveLiteral))
            predicates.Add($"cnpj_basico < '{endExclusiveLiteral}'");

        return predicates.Count == 0
            ? string.Empty
            : $" WHERE {string.Join(" AND ", predicates)}";
    }
}
