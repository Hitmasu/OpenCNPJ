using System.Text;
using System.Text.RegularExpressions;
using CNPJExporter.Modules.PortalTransparencia.Configuration;
using CNPJExporter.Modules.PortalTransparencia.Models;
using DuckDB.NET.Data;

namespace CNPJExporter.Modules.PortalTransparencia.Processors;

public sealed class ParquetProcessor
{
    private static readonly Regex DuckDbSizePattern = new(
        @"^\d+(?:\.\d+)?\s*(?:KB|MB|GB|TB)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private readonly IntegrationOptions _options;

    static ParquetProcessor()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    internal ParquetProcessor(IntegrationOptions options)
    {
        _options = options;
    }

    internal async Task ConvertToParquetAsync(
        PortalDatasetDefinition definition,
        ExtractedDataset extracted,
        string parquetPath,
        DateTimeOffset moduleUpdatedAt,
        int shardPrefixLength,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(parquetPath)!);
        var utf8Files = await ConvertInputsToUtf8Async(extracted, cancellationToken);
        var workDirectory = Path.GetDirectoryName(parquetPath)!;
        var databasePath = Path.Combine(workDirectory, $"{definition.Key}-import.duckdb");
        var temporaryParquetPath = parquetPath + ".part";

        DeleteIfExists(databasePath);
        DeleteIfExists(databasePath + ".wal");
        DeleteIfExists(temporaryParquetPath);

        try
        {
            await using (var connection = new DuckDBConnection($"Data Source={databasePath}"))
            {
                await connection.OpenAsync();
                await ConfigureDuckDbAsync(connection, workDirectory, cancellationToken);
                await ExecuteNonQueryAsync(
                    connection,
                    "CREATE OR REPLACE MACRO CleanCnpj(value) AS upper(regexp_replace(COALESCE(CAST(value AS VARCHAR), ''), '[^0-9A-Za-z]', '', 'g'))",
                    cancellationToken);
                await ValidateDatasetAsync(
                    connection,
                    definition,
                    utf8Files,
                    cancellationToken);
                await ExecuteNonQueryAsync(
                    connection,
                    BuildDatasetQuery(
                        definition,
                        utf8Files,
                        temporaryParquetPath,
                        moduleUpdatedAt,
                        Math.Max(1, shardPrefixLength)),
                    cancellationToken);
            }

            File.Move(temporaryParquetPath, parquetPath, overwrite: true);
        }
        finally
        {
            DeleteIfExists(temporaryParquetPath);
            DeleteIfExists(databasePath);
            DeleteIfExists(databasePath + ".wal");
        }
    }

    public async Task<Dictionary<string, string>> LoadHashesAsync(
        string parquetPath,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();

        long recordCount;
        await using (var countCommand = connection.CreateCommand())
        {
            countCommand.CommandText =
                $"SELECT count(*) FROM read_parquet('{EscapeSqlLiteral(parquetPath)}')";
            recordCount = Convert.ToInt64(
                await countCommand.ExecuteScalarAsync(cancellationToken));
        }

        var capacity = recordCount is > 0 and <= int.MaxValue
            ? (int)recordCount
            : 0;
        var hashes = new Dictionary<string, string>(capacity, StringComparer.Ordinal);
        await using var command = connection.CreateCommand();
        command.CommandText = $@"
            SELECT cnpj, content_hash
            FROM read_parquet('{EscapeSqlLiteral(parquetPath)}')
            WHERE cnpj IS NOT NULL AND content_hash IS NOT NULL";

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            hashes[reader.GetString(0)] = reader.GetString(1);

        return hashes;
    }

    private static string BuildDatasetQuery(
        PortalDatasetDefinition definition,
        ExtractedDataset files,
        string parquetPath,
        DateTimeOffset moduleUpdatedAt,
        int shardPrefixLength)
    {
        var updatedAt = moduleUpdatedAt.ToString("O");
        var payloadQuery = definition.Key switch
        {
            "favorecidos_pj" => BuildFavorecidosPjQuery(files, updatedAt),
            "ceis" => BuildCeisQuery(files, updatedAt),
            "cepim" => BuildCepimQuery(files, updatedAt),
            "cnep" => BuildCnepQuery(files, updatedAt),
            "acordos_leniencia" => BuildAcordosLenienciaQuery(files, updatedAt),
            _ => throw new InvalidOperationException(
                $"Processador não implementado para o dataset {definition.Key}.")
        };

        var escapedUpdatedAt = EscapeSqlLiteral(updatedAt);
        return $@"
            COPY (
                {payloadQuery}
                SELECT
                    cnpj,
                    substring(cnpj, 1, {shardPrefixLength}) AS cnpj_prefix,
                    payload_json,
                    md5(payload_json) AS content_hash,
                    '{escapedUpdatedAt}' AS source_updated_at,
                    '{escapedUpdatedAt}' AS module_updated_at
                FROM payloads
            )
            TO '{EscapeSqlLiteral(parquetPath)}'
            (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";
    }

    private static string BuildFavorecidosPjQuery(
        ExtractedDataset files,
        string updatedAt)
    {
        var companies = files.RequireFileEnding("_CNPJ.csv");
        var cnaes = files.RequireFileEnding("_CNAE.csv");
        var legalNatures = files.RequireFileEnding("_NaturezaJuridica.csv");
        var escapedUpdatedAt = EscapeSqlLiteral(updatedAt);

        return $@"
            WITH empresas AS (
                SELECT
                    CleanCnpj(cnpj) AS cnpj,
                    razao_social,
                    nome_fantasia,
                    cod_cnae,
                    cod_natureza_juridica,
                    tipo_pessoa,
                    logradouro,
                    numero,
                    complemento,
                    cep,
                    bairro,
                    municipio,
                    uf
                FROM {BuildCsvRead(
                    companies,
                    "cnpj",
                    "razao_social",
                    "nome_fantasia",
                    "cod_cnae",
                    "cod_natureza_juridica",
                    "tipo_pessoa",
                    "logradouro",
                    "numero",
                    "complemento",
                    "cep",
                    "bairro",
                    "municipio",
                    "uf")}
                WHERE regexp_full_match(CleanCnpj(cnpj), '[A-Z0-9]{{12}}[0-9]{{2}}')
            ),
            cnaes AS (
                SELECT
                    cod_secao,
                    desc_secao,
                    cod_subclasse,
                    desc_subclasse
                FROM {BuildCsvRead(
                    cnaes,
                    "cod_secao",
                    "desc_secao",
                    "cod_subclasse",
                    "desc_subclasse")}
                QUALIFY row_number() OVER (
                    PARTITION BY cod_subclasse
                    ORDER BY cod_secao, desc_subclasse
                ) = 1
            ),
            naturezas AS (
                SELECT
                    cod_natureza_juridica,
                    desc_natureza_juridica,
                    cod_tipo_natureza_juridica,
                    desc_tipo_natureza_juridica
                FROM {BuildCsvRead(
                    legalNatures,
                    "cod_natureza_juridica",
                    "desc_natureza_juridica",
                    "cod_tipo_natureza_juridica",
                    "desc_tipo_natureza_juridica")}
                QUALIFY row_number() OVER (
                    PARTITION BY cod_natureza_juridica
                    ORDER BY cod_tipo_natureza_juridica, desc_natureza_juridica
                ) = 1
            ),
            payloads AS (
                SELECT
                    empresa.cnpj,
                    to_json(struct_pack(
                        updated_at := '{escapedUpdatedAt}',
                        razao_social := COALESCE(empresa.razao_social, ''),
                        nome_fantasia := COALESCE(empresa.nome_fantasia, ''),
                        tipo_pessoa := COALESCE(empresa.tipo_pessoa, ''),
                        cnae := struct_pack(
                            codigo := COALESCE(empresa.cod_cnae, ''),
                            descricao := COALESCE(cnae.desc_subclasse, ''),
                            secao_codigo := COALESCE(cnae.cod_secao, ''),
                            secao_descricao := COALESCE(cnae.desc_secao, '')
                        ),
                        natureza_juridica := struct_pack(
                            codigo := COALESCE(empresa.cod_natureza_juridica, ''),
                            descricao := COALESCE(natureza.desc_natureza_juridica, ''),
                            tipo_codigo := COALESCE(natureza.cod_tipo_natureza_juridica, ''),
                            tipo_descricao := COALESCE(natureza.desc_tipo_natureza_juridica, '')
                        ),
                        endereco := struct_pack(
                            logradouro := COALESCE(empresa.logradouro, ''),
                            numero := COALESCE(empresa.numero, ''),
                            complemento := COALESCE(empresa.complemento, ''),
                            cep := COALESCE(empresa.cep, ''),
                            bairro := COALESCE(empresa.bairro, ''),
                            municipio := COALESCE(empresa.municipio, ''),
                            uf := COALESCE(empresa.uf, '')
                        )
                    )) AS payload_json
                FROM empresas empresa
                LEFT JOIN cnaes cnae
                    ON cnae.cod_subclasse = empresa.cod_cnae
                LEFT JOIN naturezas natureza
                    ON natureza.cod_natureza_juridica = empresa.cod_natureza_juridica
            )";
    }

    private static async Task ValidateDatasetAsync(
        DuckDBConnection connection,
        PortalDatasetDefinition definition,
        ExtractedDataset files,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(
                definition.Key,
                "favorecidos_pj",
                StringComparison.Ordinal))
        {
            return;
        }

        var companies = files.RequireFileEnding("_CNPJ.csv");
        await using var command = connection.CreateCommand();
        command.CommandText = $@"
            SELECT count(*) = count(DISTINCT CleanCnpj(cnpj))
            FROM {BuildCsvRead(
                companies,
                "cnpj",
                "razao_social",
                "nome_fantasia",
                "cod_cnae",
                "cod_natureza_juridica",
                "tipo_pessoa",
                "logradouro",
                "numero",
                "complemento",
                "cep",
                "bairro",
                "municipio",
                "uf")}
            WHERE regexp_full_match(CleanCnpj(cnpj), '[A-Z0-9]{{12}}[0-9]{{2}}')";
        var isUnique = Convert.ToBoolean(
            await command.ExecuteScalarAsync(cancellationToken));
        if (!isUnique)
        {
            throw new InvalidDataException(
                "O arquivo Favorecidos - PJ contém mais de uma linha para o mesmo CNPJ.");
        }
    }

    private static string BuildCeisQuery(
        ExtractedDataset files,
        string updatedAt)
    {
        var csv = files.RequireFileEnding("_CEIS.csv");
        var escapedUpdatedAt = EscapeSqlLiteral(updatedAt);

        return $@"
            WITH registros AS (
                SELECT
                    CleanCnpj(cpf_cnpj_sancionado) AS cnpj,
                    cadastro,
                    codigo_sancao,
                    tipo_pessoa,
                    nome_sancionado,
                    nome_informado_orgao,
                    razao_social_receita,
                    nome_fantasia_receita,
                    numero_processo,
                    categoria_sancao,
                    data_inicio_sancao,
                    data_final_sancao,
                    data_publicacao,
                    publicacao,
                    detalhamento_publicacao,
                    data_transito_julgado,
                    abrangencia_sancao,
                    orgao_sancionador,
                    uf_orgao_sancionador,
                    esfera_orgao_sancionador,
                    fundamentacao_legal,
                    data_origem_informacao,
                    origem_informacoes,
                    observacoes
                FROM {BuildCsvRead(
                    csv,
                    "cadastro",
                    "codigo_sancao",
                    "tipo_pessoa",
                    "cpf_cnpj_sancionado",
                    "nome_sancionado",
                    "nome_informado_orgao",
                    "razao_social_receita",
                    "nome_fantasia_receita",
                    "numero_processo",
                    "categoria_sancao",
                    "data_inicio_sancao",
                    "data_final_sancao",
                    "data_publicacao",
                    "publicacao",
                    "detalhamento_publicacao",
                    "data_transito_julgado",
                    "abrangencia_sancao",
                    "orgao_sancionador",
                    "uf_orgao_sancionador",
                    "esfera_orgao_sancionador",
                    "fundamentacao_legal",
                    "data_origem_informacao",
                    "origem_informacoes",
                    "observacoes")}
                WHERE regexp_full_match(
                    CleanCnpj(cpf_cnpj_sancionado),
                    '[A-Z0-9]{{12}}[0-9]{{2}}')
            ),
            payloads AS (
                SELECT
                    cnpj,
                    to_json(struct_pack(
                        updated_at := '{escapedUpdatedAt}',
                        sancoes := list(struct_pack(
                            cadastro := COALESCE(cadastro, ''),
                            codigo := COALESCE(codigo_sancao, ''),
                            tipo_pessoa := COALESCE(tipo_pessoa, ''),
                            nome_sancionado := COALESCE(nome_sancionado, ''),
                            nome_informado_orgao := COALESCE(nome_informado_orgao, ''),
                            razao_social_receita := COALESCE(razao_social_receita, ''),
                            nome_fantasia_receita := COALESCE(nome_fantasia_receita, ''),
                            numero_processo := COALESCE(numero_processo, ''),
                            categoria := COALESCE(categoria_sancao, ''),
                            data_inicio := COALESCE(data_inicio_sancao, ''),
                            data_final := COALESCE(data_final_sancao, ''),
                            data_publicacao := COALESCE(data_publicacao, ''),
                            publicacao := COALESCE(publicacao, ''),
                            detalhamento_publicacao := COALESCE(detalhamento_publicacao, ''),
                            data_transito_julgado := COALESCE(data_transito_julgado, ''),
                            abrangencia := COALESCE(abrangencia_sancao, ''),
                            orgao_sancionador := COALESCE(orgao_sancionador, ''),
                            uf_orgao_sancionador := COALESCE(uf_orgao_sancionador, ''),
                            esfera_orgao_sancionador := COALESCE(esfera_orgao_sancionador, ''),
                            fundamentacao_legal := COALESCE(fundamentacao_legal, ''),
                            data_origem_informacao := COALESCE(data_origem_informacao, ''),
                            origem_informacoes := COALESCE(origem_informacoes, ''),
                            observacoes := COALESCE(observacoes, '')
                        ) ORDER BY codigo_sancao, numero_processo, data_inicio_sancao)
                    )) AS payload_json
                FROM registros
                GROUP BY cnpj
            )";
    }

    private static string BuildCepimQuery(
        ExtractedDataset files,
        string updatedAt)
    {
        var csv = files.RequireFileEnding("_CEPIM.csv");
        var escapedUpdatedAt = EscapeSqlLiteral(updatedAt);

        return $@"
            WITH registros AS (
                SELECT
                    CleanCnpj(cnpj_entidade) AS cnpj,
                    nome_entidade,
                    numero_convenio,
                    orgao_concedente,
                    motivo_impedimento
                FROM {BuildCsvRead(
                    csv,
                    "cnpj_entidade",
                    "nome_entidade",
                    "numero_convenio",
                    "orgao_concedente",
                    "motivo_impedimento")}
                WHERE regexp_full_match(
                    CleanCnpj(cnpj_entidade),
                    '[A-Z0-9]{{12}}[0-9]{{2}}')
            ),
            payloads AS (
                SELECT
                    cnpj,
                    to_json(struct_pack(
                        updated_at := '{escapedUpdatedAt}',
                        impedimentos := list(struct_pack(
                            nome_entidade := COALESCE(nome_entidade, ''),
                            numero_convenio := COALESCE(numero_convenio, ''),
                            orgao_concedente := COALESCE(orgao_concedente, ''),
                            motivo := COALESCE(motivo_impedimento, '')
                        ) ORDER BY numero_convenio, orgao_concedente, motivo_impedimento)
                    )) AS payload_json
                FROM registros
                GROUP BY cnpj
            )";
    }

    private static string BuildCnepQuery(
        ExtractedDataset files,
        string updatedAt)
    {
        var csv = files.RequireFileEnding("_CNEP.csv");
        var escapedUpdatedAt = EscapeSqlLiteral(updatedAt);

        return $@"
            WITH registros AS (
                SELECT
                    CleanCnpj(cpf_cnpj_sancionado) AS cnpj,
                    cadastro,
                    codigo_sancao,
                    tipo_pessoa,
                    nome_sancionado,
                    nome_informado_orgao,
                    razao_social_receita,
                    nome_fantasia_receita,
                    numero_processo,
                    categoria_sancao,
                    valor_multa,
                    data_inicio_sancao,
                    data_final_sancao,
                    data_publicacao,
                    publicacao,
                    detalhamento_publicacao,
                    data_transito_julgado,
                    abrangencia_sancao,
                    orgao_sancionador,
                    uf_orgao_sancionador,
                    esfera_orgao_sancionador,
                    fundamentacao_legal,
                    data_origem_informacao,
                    origem_informacoes,
                    observacoes
                FROM {BuildCsvRead(
                    csv,
                    "cadastro",
                    "codigo_sancao",
                    "tipo_pessoa",
                    "cpf_cnpj_sancionado",
                    "nome_sancionado",
                    "nome_informado_orgao",
                    "razao_social_receita",
                    "nome_fantasia_receita",
                    "numero_processo",
                    "categoria_sancao",
                    "valor_multa",
                    "data_inicio_sancao",
                    "data_final_sancao",
                    "data_publicacao",
                    "publicacao",
                    "detalhamento_publicacao",
                    "data_transito_julgado",
                    "abrangencia_sancao",
                    "orgao_sancionador",
                    "uf_orgao_sancionador",
                    "esfera_orgao_sancionador",
                    "fundamentacao_legal",
                    "data_origem_informacao",
                    "origem_informacoes",
                    "observacoes")}
                WHERE regexp_full_match(
                    CleanCnpj(cpf_cnpj_sancionado),
                    '[A-Z0-9]{{12}}[0-9]{{2}}')
            ),
            payloads AS (
                SELECT
                    cnpj,
                    to_json(struct_pack(
                        updated_at := '{escapedUpdatedAt}',
                        sancoes := list(struct_pack(
                            cadastro := COALESCE(cadastro, ''),
                            codigo := COALESCE(codigo_sancao, ''),
                            tipo_pessoa := COALESCE(tipo_pessoa, ''),
                            nome_sancionado := COALESCE(nome_sancionado, ''),
                            nome_informado_orgao := COALESCE(nome_informado_orgao, ''),
                            razao_social_receita := COALESCE(razao_social_receita, ''),
                            nome_fantasia_receita := COALESCE(nome_fantasia_receita, ''),
                            numero_processo := COALESCE(numero_processo, ''),
                            categoria := COALESCE(categoria_sancao, ''),
                            valor_multa := COALESCE(valor_multa, ''),
                            data_inicio := COALESCE(data_inicio_sancao, ''),
                            data_final := COALESCE(data_final_sancao, ''),
                            data_publicacao := COALESCE(data_publicacao, ''),
                            publicacao := COALESCE(publicacao, ''),
                            detalhamento_publicacao := COALESCE(detalhamento_publicacao, ''),
                            data_transito_julgado := COALESCE(data_transito_julgado, ''),
                            abrangencia := COALESCE(abrangencia_sancao, ''),
                            orgao_sancionador := COALESCE(orgao_sancionador, ''),
                            uf_orgao_sancionador := COALESCE(uf_orgao_sancionador, ''),
                            esfera_orgao_sancionador := COALESCE(esfera_orgao_sancionador, ''),
                            fundamentacao_legal := COALESCE(fundamentacao_legal, ''),
                            data_origem_informacao := COALESCE(data_origem_informacao, ''),
                            origem_informacoes := COALESCE(origem_informacoes, ''),
                            observacoes := COALESCE(observacoes, '')
                        ) ORDER BY codigo_sancao, numero_processo, data_inicio_sancao)
                    )) AS payload_json
                FROM registros
                GROUP BY cnpj
            )";
    }

    private static string BuildAcordosLenienciaQuery(
        ExtractedDataset files,
        string updatedAt)
    {
        var agreements = files.RequireFileEnding("_Acordos.csv");
        var effects = files.RequireFileEnding("_Efeitos.csv");
        var escapedUpdatedAt = EscapeSqlLiteral(updatedAt);

        return $@"
            WITH efeitos AS (
                SELECT
                    id_acordo,
                    list(struct_pack(
                        efeito := COALESCE(efeito, ''),
                        complemento := COALESCE(complemento, '')
                    ) ORDER BY efeito, complemento) AS itens
                FROM {BuildCsvRead(
                    effects,
                    "id_acordo",
                    "efeito",
                    "complemento")}
                GROUP BY id_acordo
            ),
            acordos AS (
                SELECT
                    CleanCnpj(cnpj_sancionado) AS cnpj,
                    id_acordo,
                    razao_social_receita,
                    nome_fantasia_receita,
                    data_inicio,
                    data_fim,
                    situacao,
                    data_informacao,
                    numero_processo,
                    termos_acordo,
                    orgao_sancionador
                FROM {BuildCsvRead(
                    agreements,
                    "id_acordo",
                    "cnpj_sancionado",
                    "razao_social_receita",
                    "nome_fantasia_receita",
                    "data_inicio",
                    "data_fim",
                    "situacao",
                    "data_informacao",
                    "numero_processo",
                    "termos_acordo",
                    "orgao_sancionador")}
                WHERE regexp_full_match(
                    CleanCnpj(cnpj_sancionado),
                    '[A-Z0-9]{{12}}[0-9]{{2}}')
            ),
            acordos_com_efeitos AS (
                SELECT
                    acordo.*,
                    COALESCE(efeito.itens, []) AS efeitos
                FROM acordos acordo
                LEFT JOIN efeitos efeito
                    ON efeito.id_acordo = acordo.id_acordo
            ),
            payloads AS (
                SELECT
                    cnpj,
                    to_json(struct_pack(
                        updated_at := '{escapedUpdatedAt}',
                        acordos := list(struct_pack(
                            id := COALESCE(id_acordo, ''),
                            razao_social_receita := COALESCE(razao_social_receita, ''),
                            nome_fantasia_receita := COALESCE(nome_fantasia_receita, ''),
                            data_inicio := COALESCE(data_inicio, ''),
                            data_fim := COALESCE(data_fim, ''),
                            situacao := COALESCE(situacao, ''),
                            data_informacao := COALESCE(data_informacao, ''),
                            numero_processo := COALESCE(numero_processo, ''),
                            termos := COALESCE(termos_acordo, ''),
                            orgao_sancionador := COALESCE(orgao_sancionador, ''),
                            efeitos := efeitos
                        ) ORDER BY id_acordo, numero_processo)
                    )) AS payload_json
                FROM acordos_com_efeitos
                GROUP BY cnpj
            )";
    }

    internal static string BuildCsvRead(string path, params string[] columns)
    {
        var columnMap = string.Join(
            ", ",
            columns.Select(column => $"'{column}': 'VARCHAR'"));
        return $@"
            read_csv(
                '{EscapeSqlLiteral(path)}',
                columns = {{{columnMap}}},
                header = true,
                auto_detect = false,
                all_varchar = true,
                delim = ';',
                quote = '""',
                escape = '""',
                nullstr = '',
                strict_mode = true,
                ignore_errors = false,
                parallel = true
            )";
    }

    internal static async Task<ExtractedDataset> ConvertInputsToUtf8Async(
        ExtractedDataset files,
        CancellationToken cancellationToken)
    {
        var converted = new List<string>(files.CsvPaths.Count);
        foreach (var sourcePath in files.CsvPaths)
        {
            var outputDirectory = Path.Combine(Path.GetDirectoryName(sourcePath)!, "_utf8");
            var destinationPath = Path.Combine(outputDirectory, Path.GetFileName(sourcePath));
            converted.Add(
                await ConvertWindows1252FileToUtf8Async(
                    sourcePath,
                    destinationPath,
                    cancellationToken));
        }

        return new ExtractedDataset(converted);
    }

    private static async Task<string> ConvertWindows1252FileToUtf8Async(
        string sourcePath,
        string destinationPath,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(destinationPath)!);

        if (File.Exists(destinationPath)
            && File.GetLastWriteTimeUtc(destinationPath) >= File.GetLastWriteTimeUtc(sourcePath)
            && new FileInfo(destinationPath).Length > 0)
        {
            return destinationPath;
        }

        await using var input = new FileStream(
            sourcePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            1 << 20,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var output = new FileStream(
            destinationPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.Read,
            1 << 20,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        using var reader = new StreamReader(
            input,
            Encoding.GetEncoding(1252),
            detectEncodingFromByteOrderMarks: true,
            bufferSize: 1 << 20,
            leaveOpen: true);
        await using var writer = new StreamWriter(
            output,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            bufferSize: 1 << 20,
            leaveOpen: true);

        var buffer = new char[1 << 20];
        int read;
        while ((read = await reader.ReadAsync(
                   buffer.AsMemory(0, buffer.Length),
                   cancellationToken)) > 0)
        {
            await writer.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
        }

        return destinationPath;
    }

    internal async Task ConfigureDuckDbAsync(
        DuckDBConnection connection,
        string workDirectory,
        CancellationToken cancellationToken)
    {
        var memoryLimit = ValidateDuckDbSize(
            _options.DuckDbMemoryLimit,
            nameof(_options.DuckDbMemoryLimit));
        var maxTemporarySize = ValidateDuckDbSize(
            _options.DuckDbMaxTempDirectorySize,
            nameof(_options.DuckDbMaxTempDirectorySize));
        var temporaryDirectory = Path.Combine(workDirectory, "_duckdb_temp");
        Directory.CreateDirectory(temporaryDirectory);

        await ExecuteNonQueryAsync(
            connection,
            $@"
                SET preserve_insertion_order = false;
                SET threads = {Math.Max(1, _options.DuckDbThreads)};
                SET memory_limit = '{EscapeSqlLiteral(memoryLimit)}';
                SET temp_directory = '{EscapeSqlLiteral(temporaryDirectory)}';
                SET max_temp_directory_size = '{EscapeSqlLiteral(maxTemporarySize)}';",
            cancellationToken);
    }

    private static string ValidateDuckDbSize(string value, string settingName)
    {
        var normalized = value?.Trim() ?? string.Empty;
        if (!DuckDbSizePattern.IsMatch(normalized))
        {
            throw new InvalidOperationException(
                $"{settingName} deve usar um tamanho explícito, por exemplo 512MB ou 20GB.");
        }

        return normalized;
    }

    internal static async Task ExecuteNonQueryAsync(
        DuckDBConnection connection,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    internal static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
