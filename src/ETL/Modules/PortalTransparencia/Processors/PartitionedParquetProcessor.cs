using CNPJExporter.Modules.PortalTransparencia.Configuration;
using CNPJExporter.Modules.PortalTransparencia.Models;
using DuckDB.NET.Data;

namespace CNPJExporter.Modules.PortalTransparencia.Processors;

internal sealed record PartitionedParquetResult(
    string ParquetGlob,
    long RecordCount,
    DateTimeOffset UpdatedAt);

/// <summary>
/// Materializes the large Portal da Transparência datasets in bounded steps:
/// one projection per source archive into CNPJ hash buckets, followed by
/// record-level subpartitions before aggregation. This avoids holding either
/// a complete annual source or a very large CNPJ payload in DuckDB memory.
/// </summary>
internal sealed class PartitionedParquetProcessor
{
    private const string CnpjPattern = "[A-Z0-9]{12}[0-9]{2}";
    private const int HistoricalRecordPartitions = 32;

    private readonly IntegrationOptions _options;
    private readonly ParquetProcessor _duckDbSupport;

    public PartitionedParquetProcessor(IntegrationOptions options)
    {
        _options = options;
        _duckDbSupport = new ParquetProcessor(options);
    }

    public async Task<PartitionedParquetResult> ConvertAsync(
        PortalDatasetDefinition definition,
        ExtractedDataset extracted,
        string outputDirectory,
        DateTimeOffset updatedAt,
        int shardPrefixLength,
        CancellationToken cancellationToken = default)
    {
        var collectionProperty = ResolveCollectionProperty(definition);
        var cnpjPartitions = NormalizePartitionCount(
            _options.ProcessingPartitions);
        var recordPartitions = HistoricalRecordPartitions;
        var utf8Files = await ParquetProcessor.ConvertInputsToUtf8Async(
            extracted,
            cancellationToken);
        var parentDirectory = Path.GetDirectoryName(outputDirectory)
                              ?? throw new InvalidOperationException(
                                  "O diretório de saída segmentada é inválido.");
        var stagingDirectory = outputDirectory + ".staging";
        var databasePath = outputDirectory + ".duckdb";

        DeleteDirectory(outputDirectory);
        DeleteDirectory(stagingDirectory);
        DeleteFile(databasePath);
        DeleteFile(databasePath + ".wal");
        Directory.CreateDirectory(parentDirectory);

        try
        {
            await using var connection = new DuckDBConnection(
                $"Data Source={databasePath}");
            await connection.OpenAsync();
            await _duckDbSupport.ConfigureDuckDbAsync(
                connection,
                parentDirectory,
                cancellationToken);
            await ParquetProcessor.ExecuteNonQueryAsync(
                connection,
                """
                SET partitioned_write_max_open_files = 8;
                CREATE OR REPLACE MACRO CleanCnpj(value) AS upper(regexp_replace(COALESCE(CAST(value AS VARCHAR), ''), '[^0-9A-Za-z]', '', 'g'));
                """,
                cancellationToken);

            Directory.CreateDirectory(stagingDirectory);
            var sourceBatches = SplitSourceBatches(definition, utf8Files);
            for (var batchIndex = 0;
                 batchIndex < sourceBatches.Count;
                 batchIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var eventsQueries = BuildEventsQueries(
                    definition,
                    sourceBatches[batchIndex]);
                for (var queryIndex = 0;
                     queryIndex < eventsQueries.Count;
                     queryIndex++)
                {
                    var eventsQuery = eventsQueries[queryIndex];
                    var batchDirectory = Path.Combine(
                        stagingDirectory,
                        $"batch-{batchIndex:D3}-{queryIndex:D2}");
                    await ParquetProcessor.ExecuteNonQueryAsync(
                        connection,
                        $"""
                         COPY (
                             SELECT
                                 cnpj,
                                 record_sort,
                                 record_json,
                                 CAST(hash(cnpj) % {cnpjPartitions} AS INTEGER) AS bucket
                             FROM (
                                 {eventsQuery}
                             ) source_events
                             WHERE regexp_full_match(cnpj, '{CnpjPattern}')
                         )
                         TO '{Sql(batchDirectory)}'
                         (
                             FORMAT PARQUET,
                             COMPRESSION ZSTD,
                             PARTITION_BY (bucket)
                         )
                         """,
                        cancellationToken);
                }
            }

            // Aggregation is intentionally single-threaded. DuckDB accounts
            // per-thread buffers against the same memory limit, and the wide
            // JSON lists from licitações/notas can otherwise exceed 512 MB
            // even after hash partitioning.
            await ParquetProcessor.ExecuteNonQueryAsync(
                connection,
                "SET threads = 1;",
                cancellationToken);
            var stagingFilesByBucket = IndexStagingFilesByBucket(
                stagingDirectory);
            Directory.CreateDirectory(outputDirectory);
            var escapedUpdatedAt = Sql(updatedAt.ToString("O"));
            for (var partition = 0;
                 partition < cnpjPartitions;
                 partition++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!stagingFilesByBucket.TryGetValue(
                        partition,
                        out var bucketFiles))
                    continue;

                var bucketSources = string.Join(
                    ", ",
                    bucketFiles.Select(path => $"'{Sql(path)}'"));
                for (var recordPartition = 0;
                     recordPartition < recordPartitions;
                     recordPartition++)
                {
                    var recordFilter = recordPartitions == 1
                        ? string.Empty
                        : $"WHERE hash(record_sort, record_json) % {recordPartitions} = {recordPartition}";
                    var outputPath = Path.Combine(
                        outputDirectory,
                        $"part-{partition:D3}-{recordPartition:D2}.parquet");
                    await ParquetProcessor.ExecuteNonQueryAsync(
                        connection,
                        $"""
                         COPY (
                             WITH payloads AS (
                                 SELECT
                                     cnpj,
                                     to_json(struct_pack(
                                         updated_at := '{escapedUpdatedAt}',
                                         {collectionProperty} := list(
                                             CAST(record_json AS JSON)
                                             ORDER BY record_sort, record_json)
                                     )) AS payload_json
                                 FROM read_parquet([{bucketSources}])
                                 {recordFilter}
                                 GROUP BY cnpj
                             )
                             SELECT
                                 cnpj,
                                 substring(cnpj, 1, {Math.Max(1, shardPrefixLength)}) AS cnpj_prefix,
                                 payload_json,
                                 md5(CAST(payload_json AS VARCHAR)) AS content_hash,
                                 '{escapedUpdatedAt}' AS source_updated_at,
                                 '{escapedUpdatedAt}' AS module_updated_at
                             FROM payloads
                             ORDER BY cnpj
                         )
                         TO '{Sql(outputPath)}'
                         (FORMAT PARQUET, COMPRESSION ZSTD)
                         """,
                        cancellationToken);
                }
            }

            var parquetGlob = Path.Combine(outputDirectory, "*.parquet");
            var recordCount = await CountRecordsAsync(
                connection,
                parquetGlob,
                cancellationToken);
            await File.WriteAllTextAsync(
                GetCompletionMarker(outputDirectory),
                $"{recordCount}|{updatedAt:O}",
                cancellationToken);
            return new PartitionedParquetResult(
                parquetGlob,
                recordCount,
                updatedAt);
        }
        finally
        {
            DeleteDirectory(stagingDirectory);
            DeleteFile(databasePath);
            DeleteFile(databasePath + ".wal");
        }
    }

    internal static async Task<PartitionedParquetResult?> TryLoadCompletedAsync(
        string outputDirectory,
        CancellationToken cancellationToken = default)
    {
        var marker = GetCompletionMarker(outputDirectory);
        if (!File.Exists(marker))
            return null;

        var content = await File.ReadAllTextAsync(marker, cancellationToken);
        var separator = content.IndexOf('|');
        if (separator <= 0
            || !long.TryParse(
                content.AsSpan(0, separator),
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out var recordCount)
            || !DateTimeOffset.TryParse(
                content.AsSpan(separator + 1),
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind,
                out var updatedAt))
        {
            return null;
        }

        var parquetGlob = Path.Combine(outputDirectory, "*.parquet");
        if (!Directory.Exists(outputDirectory)
            || !Directory.EnumerateFiles(
                    outputDirectory,
                    "*.parquet",
                    SearchOption.TopDirectoryOnly)
                .Any())
        {
            return null;
        }

        return new PartitionedParquetResult(
            parquetGlob,
            recordCount,
            updatedAt);
    }

    private static async Task<long> CountRecordsAsync(
        DuckDBConnection connection,
        string parquetGlob,
        CancellationToken cancellationToken)
    {
        if (!Directory.Exists(Path.GetDirectoryName(parquetGlob))
            || !Directory.EnumerateFiles(
                    Path.GetDirectoryName(parquetGlob)!,
                    "*.parquet",
                    SearchOption.TopDirectoryOnly)
                .Any())
        {
            return 0;
        }

        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT count(DISTINCT cnpj) FROM read_parquet('{Sql(parquetGlob)}')";
        return Convert.ToInt64(
            await command.ExecuteScalarAsync(cancellationToken));
    }

    private static string ResolveCollectionProperty(
        PortalDatasetDefinition definition) =>
        definition.SegmentCollectionProperty
        ?? definition.Key switch
        {
            "convenios" => "convenios",
            "emendas_parlamentares" => "emendas",
            _ => throw new InvalidOperationException(
                $"O dataset {definition.Key} não possui coleção particionada.")
        };

    private static int NormalizePartitionCount(int requested)
    {
        var maximum = Math.Clamp(requested, 1, 64);
        var normalized = 1;
        while (normalized <= maximum / 2)
            normalized *= 2;
        return normalized;
    }

    private static IReadOnlyList<ExtractedDataset> SplitSourceBatches(
        PortalDatasetDefinition definition,
        ExtractedDataset files)
    {
        var batches = new List<ExtractedDataset>();
        foreach (var filesByDirectory in files.CsvPaths
                     .GroupBy(
                         path => Path.GetDirectoryName(path) ?? string.Empty,
                         StringComparer.Ordinal)
                     .OrderBy(group => group.Key, StringComparer.Ordinal))
        {
            var pathsBySuffix = definition.RequiredCsvSuffixes
                .Select(suffix => filesByDirectory
                    .Where(path => Path.GetFileName(path).EndsWith(
                        suffix,
                        StringComparison.OrdinalIgnoreCase))
                    .OrderBy(path => path, StringComparer.Ordinal)
                    .ToArray())
                .ToArray();
            if (pathsBySuffix.Any(paths => paths.Length == 0))
            {
                throw new InvalidOperationException(
                    $"O lote {filesByDirectory.Key} de {definition.Key} não contém todos os CSVs obrigatórios.");
            }

            var batchCount = pathsBySuffix[0].Length;
            if (pathsBySuffix.Any(paths => paths.Length != batchCount))
            {
                throw new InvalidOperationException(
                    $"O lote {filesByDirectory.Key} de {definition.Key} contém quantidades incompatíveis de CSVs.");
            }

            for (var batchIndex = 0;
                 batchIndex < batchCount;
                 batchIndex++)
            {
                batches.Add(new ExtractedDataset(
                    pathsBySuffix
                        .Select(paths => paths[batchIndex])
                        .ToArray()));
            }
        }

        if (batches.Count == 0)
        {
            throw new InvalidOperationException(
                $"Nenhum lote de CSV foi encontrado para {definition.Key}.");
        }

        return batches;
    }

    private static IReadOnlyDictionary<int, IReadOnlyList<string>>
        IndexStagingFilesByBucket(string stagingDirectory)
    {
        return Directory
            .EnumerateFiles(
                stagingDirectory,
                "*.parquet",
                SearchOption.AllDirectories)
            .Select(path => new
            {
                Path = path,
                DirectoryName = new DirectoryInfo(
                    Path.GetDirectoryName(path)!).Name
            })
            .Where(entry => entry.DirectoryName.StartsWith(
                "bucket=",
                StringComparison.Ordinal))
            .Select(entry => new
            {
                entry.Path,
                Parsed = int.TryParse(
                    entry.DirectoryName.AsSpan("bucket=".Length),
                    System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out var bucket),
                Bucket = bucket
            })
            .Where(entry => entry.Parsed)
            .GroupBy(entry => entry.Bucket)
            .ToDictionary(
                group => group.Key,
                group => (IReadOnlyList<string>)group
                    .Select(entry => entry.Path)
                    .OrderBy(path => path, StringComparer.Ordinal)
                    .ToArray());
    }

    private static string BuildEventsQuery(
        PortalDatasetDefinition definition,
        ExtractedDataset files) =>
        definition.Key switch
        {
            "licitacoes" => BuildLicitacoesQuery(files),
            "contratos" => BuildContratosQuery(files),
            "renuncias_fiscais" => BuildRenunciasQuery(files),
            "notas_fiscais" => BuildNotasFiscaisQuery(files),
            "convenios" => BuildConveniosQuery(files),
            "emendas_documentos" => BuildEmendasDocumentosQuery(files),
            _ => throw new InvalidOperationException(
                $"Processamento particionado não implementado para {definition.Key}.")
        };

    private static IReadOnlyList<string> BuildEventsQueries(
        PortalDatasetDefinition definition,
        ExtractedDataset files) =>
        definition.Key == "emendas_parlamentares"
            ? BuildEmendasQueries(files)
            : [BuildEventsQuery(definition, files)];

    private static string BuildLicitacoesQuery(ExtractedDataset files)
    {
        var licitacoes = Csv(
            files.RequireFilesEnding("_Licitação.csv"),
            "numero_licitacao",
            "codigo_ug",
            "nome_ug",
            "codigo_modalidade",
            "modalidade",
            "numero_processo",
            "objeto",
            "situacao",
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao",
            "nome_orgao",
            "uf",
            "municipio",
            "data_resultado",
            "data_abertura",
            "valor");
        var itens = Csv(
            files.RequireFilesEnding("_ItemLicitação.csv"),
            "numero_licitacao",
            "codigo_ug",
            "nome_ug",
            "codigo_modalidade",
            "modalidade",
            "numero_processo",
            "codigo_orgao",
            "nome_orgao",
            "codigo_item",
            "descricao",
            "quantidade",
            "valor_item",
            "codigo_vencedor",
            "nome_vencedor");
        var participantes = Csv(
            files.RequireFilesEnding("_ParticipantesLicitação.csv"),
            "numero_licitacao",
            "codigo_ug",
            "nome_ug",
            "codigo_modalidade",
            "modalidade",
            "numero_processo",
            "codigo_orgao",
            "nome_orgao",
            "codigo_item",
            "descricao_item",
            "codigo_participante",
            "nome_participante",
            "flag_vencedor");
        var empenhos = Csv(
            files.RequireFilesEnding("_EmpenhosRelacionados.csv"),
            "numero_licitacao",
            "codigo_ug",
            "nome_ug",
            "codigo_modalidade",
            "modalidade",
            "numero_processo",
            "codigo_empenho",
            "data_emissao",
            "observacao",
            "valor_empenho");

        return $"""
                WITH
                licitacoes AS (SELECT * FROM {licitacoes}),
                itens AS (SELECT * FROM {itens}),
                participantes AS (SELECT * FROM {participantes}),
                empenhos AS (SELECT * FROM {empenhos}),
                vinculos AS (
                    SELECT DISTINCT
                        CleanCnpj(codigo_participante) AS cnpj,
                        numero_licitacao,
                        codigo_ug,
                        codigo_modalidade,
                        numero_processo
                    FROM participantes
                    WHERE {Valid("CleanCnpj(codigo_participante)")}
                    UNION
                    SELECT DISTINCT
                        CleanCnpj(codigo_vencedor) AS cnpj,
                        numero_licitacao,
                        codigo_ug,
                        codigo_modalidade,
                        numero_processo
                    FROM itens
                    WHERE {Valid("CleanCnpj(codigo_vencedor)")}
                ),
                eventos AS (
                    SELECT
                        vinculo.cnpj,
                        concat('0|', COALESCE(licitacao.numero_licitacao, ''), '|', COALESCE(licitacao.numero_processo, '')) AS record_sort,
                        to_json(struct_pack(
                            tipo_registro := 'licitacao',
                            numero := COALESCE(licitacao.numero_licitacao, ''),
                            numero_processo := COALESCE(licitacao.numero_processo, ''),
                            objeto := COALESCE(licitacao.objeto, ''),
                            situacao := COALESCE(licitacao.situacao, ''),
                            modalidade_codigo := COALESCE(licitacao.codigo_modalidade, ''),
                            modalidade := COALESCE(licitacao.modalidade, ''),
                            ug_codigo := COALESCE(licitacao.codigo_ug, ''),
                            ug_nome := COALESCE(licitacao.nome_ug, ''),
                            orgao_codigo := COALESCE(licitacao.codigo_orgao, ''),
                            orgao_nome := COALESCE(licitacao.nome_orgao, ''),
                            orgao_superior_codigo := COALESCE(licitacao.codigo_orgao_superior, ''),
                            orgao_superior_nome := COALESCE(licitacao.nome_orgao_superior, ''),
                            uf := COALESCE(licitacao.uf, ''),
                            municipio := COALESCE(licitacao.municipio, ''),
                            data_resultado := COALESCE(licitacao.data_resultado, ''),
                            data_abertura := COALESCE(licitacao.data_abertura, ''),
                            valor := COALESCE(licitacao.valor, '')
                        )) AS record_json
                    FROM vinculos vinculo
                    INNER JOIN licitacoes licitacao USING (
                        numero_licitacao,
                        codigo_ug,
                        codigo_modalidade,
                        numero_processo)

                    UNION ALL

                    SELECT
                        CleanCnpj(codigo_participante),
                        concat('1|', COALESCE(numero_licitacao, ''), '|', COALESCE(codigo_item, ''), '|', COALESCE(codigo_participante, '')),
                        to_json(struct_pack(
                            tipo_registro := 'participacao',
                            numero := COALESCE(numero_licitacao, ''),
                            numero_processo := COALESCE(numero_processo, ''),
                            modalidade_codigo := COALESCE(codigo_modalidade, ''),
                            modalidade := COALESCE(modalidade, ''),
                            ug_codigo := COALESCE(codigo_ug, ''),
                            ug_nome := COALESCE(nome_ug, ''),
                            orgao_codigo := COALESCE(codigo_orgao, ''),
                            orgao_nome := COALESCE(nome_orgao, ''),
                            item_codigo := COALESCE(codigo_item, ''),
                            item_descricao := COALESCE(descricao_item, ''),
                            participante_nome := COALESCE(nome_participante, ''),
                            vencedor := COALESCE(flag_vencedor, '')
                        ))
                    FROM participantes
                    WHERE {Valid("CleanCnpj(codigo_participante)")}

                    UNION ALL

                    SELECT
                        CleanCnpj(codigo_vencedor),
                        concat('2|', COALESCE(numero_licitacao, ''), '|', COALESCE(codigo_item, ''), '|', COALESCE(codigo_vencedor, '')),
                        to_json(struct_pack(
                            tipo_registro := 'item_vencido',
                            numero := COALESCE(numero_licitacao, ''),
                            numero_processo := COALESCE(numero_processo, ''),
                            modalidade_codigo := COALESCE(codigo_modalidade, ''),
                            modalidade := COALESCE(modalidade, ''),
                            ug_codigo := COALESCE(codigo_ug, ''),
                            ug_nome := COALESCE(nome_ug, ''),
                            orgao_codigo := COALESCE(codigo_orgao, ''),
                            orgao_nome := COALESCE(nome_orgao, ''),
                            item_codigo := COALESCE(codigo_item, ''),
                            item_descricao := COALESCE(descricao, ''),
                            quantidade := COALESCE(quantidade, ''),
                            valor_item := COALESCE(valor_item, ''),
                            vencedor_nome := COALESCE(nome_vencedor, '')
                        ))
                    FROM itens
                    WHERE {Valid("CleanCnpj(codigo_vencedor)")}

                    UNION ALL

                    SELECT
                        vinculo.cnpj,
                        concat('3|', COALESCE(empenho.numero_licitacao, ''), '|', COALESCE(empenho.codigo_empenho, '')),
                        to_json(struct_pack(
                            tipo_registro := 'empenho',
                            numero := COALESCE(empenho.numero_licitacao, ''),
                            numero_processo := COALESCE(empenho.numero_processo, ''),
                            modalidade_codigo := COALESCE(empenho.codigo_modalidade, ''),
                            modalidade := COALESCE(empenho.modalidade, ''),
                            ug_codigo := COALESCE(empenho.codigo_ug, ''),
                            ug_nome := COALESCE(empenho.nome_ug, ''),
                            empenho_codigo := COALESCE(empenho.codigo_empenho, ''),
                            data_emissao := COALESCE(empenho.data_emissao, ''),
                            observacao := COALESCE(empenho.observacao, ''),
                            valor_empenho := COALESCE(empenho.valor_empenho, '')
                        ))
                    FROM empenhos empenho
                    INNER JOIN vinculos vinculo USING (
                        numero_licitacao,
                        codigo_ug,
                        codigo_modalidade,
                        numero_processo)
                )
                SELECT cnpj, record_sort, record_json
                FROM eventos
                """;
    }

    private static string BuildContratosQuery(ExtractedDataset files)
    {
        var contratos = Csv(
            files.RequireFilesEnding("_Compras.csv"),
            "numero_contrato",
            "objeto",
            "fundamento_legal",
            "modalidade",
            "situacao",
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao",
            "nome_orgao",
            "codigo_ug",
            "nome_ug",
            "data_assinatura",
            "data_publicacao",
            "data_inicio",
            "data_fim",
            "codigo_contratado",
            "nome_contratado",
            "valor_inicial",
            "valor_final",
            "numero_licitacao",
            "codigo_ug_licitacao",
            "nome_ug_licitacao",
            "codigo_modalidade_licitacao",
            "modalidade_licitacao");
        var itens = Csv(
            files.RequireFilesEnding("_ItemCompra.csv"),
            "codigo_orgao",
            "nome_orgao",
            "codigo_ug",
            "nome_ug",
            "numero_contrato",
            "codigo_item",
            "descricao_item",
            "descricao_complementar",
            "quantidade",
            "valor_item");
        var termos = Csv(
            files.RequireFilesEnding("_TermoAditivo.csv"),
            "numero_contrato",
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao",
            "nome_orgao",
            "codigo_ug",
            "nome_ug",
            "numero_termo",
            "data_publicacao",
            "objeto");
        var apostilamentos = Csv(
            files.RequireFilesEnding("_Apostilamento.csv"),
            "numero_contrato",
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao",
            "nome_orgao",
            "codigo_ug",
            "nome_ug",
            "numero_apostilamento",
            "descricao",
            "valor",
            "data_inclusao",
            "situacao");

        return $"""
                WITH
                contratos AS (SELECT * FROM {contratos}),
                itens AS (SELECT * FROM {itens}),
                termos AS (SELECT * FROM {termos}),
                apostilamentos AS (SELECT * FROM {apostilamentos}),
                vinculos AS (
                    SELECT DISTINCT
                        CleanCnpj(codigo_contratado) AS cnpj,
                        numero_contrato,
                        codigo_orgao,
                        codigo_ug
                    FROM contratos
                    WHERE {Valid("CleanCnpj(codigo_contratado)")}
                ),
                eventos AS (
                    SELECT
                        CleanCnpj(codigo_contratado) AS cnpj,
                        concat('0|', COALESCE(numero_contrato, ''), '|', COALESCE(codigo_orgao, ''), '|', COALESCE(codigo_ug, '')) AS record_sort,
                        to_json(struct_pack(
                            tipo_registro := 'contrato',
                            numero := COALESCE(numero_contrato, ''),
                            objeto := COALESCE(objeto, ''),
                            fundamento_legal := COALESCE(fundamento_legal, ''),
                            modalidade := COALESCE(modalidade, ''),
                            situacao := COALESCE(situacao, ''),
                            orgao_superior_codigo := COALESCE(codigo_orgao_superior, ''),
                            orgao_superior_nome := COALESCE(nome_orgao_superior, ''),
                            orgao_codigo := COALESCE(codigo_orgao, ''),
                            orgao_nome := COALESCE(nome_orgao, ''),
                            ug_codigo := COALESCE(codigo_ug, ''),
                            ug_nome := COALESCE(nome_ug, ''),
                            contratado_nome := COALESCE(nome_contratado, ''),
                            data_assinatura := COALESCE(data_assinatura, ''),
                            data_publicacao := COALESCE(data_publicacao, ''),
                            data_inicio := COALESCE(data_inicio, ''),
                            data_fim := COALESCE(data_fim, ''),
                            valor_inicial := COALESCE(valor_inicial, ''),
                            valor_final := COALESCE(valor_final, ''),
                            licitacao_numero := COALESCE(numero_licitacao, ''),
                            licitacao_ug_codigo := COALESCE(codigo_ug_licitacao, ''),
                            licitacao_ug_nome := COALESCE(nome_ug_licitacao, ''),
                            licitacao_modalidade_codigo := COALESCE(codigo_modalidade_licitacao, ''),
                            licitacao_modalidade := COALESCE(modalidade_licitacao, '')
                        )) AS record_json
                    FROM contratos
                    WHERE {Valid("CleanCnpj(codigo_contratado)")}

                    UNION ALL

                    SELECT
                        vinculo.cnpj,
                        concat('1|', COALESCE(item.numero_contrato, ''), '|', COALESCE(item.codigo_item, '')),
                        to_json(struct_pack(
                            tipo_registro := 'item',
                            numero := COALESCE(item.numero_contrato, ''),
                            orgao_codigo := COALESCE(item.codigo_orgao, ''),
                            orgao_nome := COALESCE(item.nome_orgao, ''),
                            ug_codigo := COALESCE(item.codigo_ug, ''),
                            ug_nome := COALESCE(item.nome_ug, ''),
                            item_codigo := COALESCE(item.codigo_item, ''),
                            item_descricao := COALESCE(item.descricao_item, ''),
                            item_descricao_complementar := COALESCE(item.descricao_complementar, ''),
                            quantidade := COALESCE(item.quantidade, ''),
                            valor_item := COALESCE(item.valor_item, '')
                        ))
                    FROM itens item
                    INNER JOIN vinculos vinculo USING (
                        numero_contrato,
                        codigo_orgao,
                        codigo_ug)

                    UNION ALL

                    SELECT
                        vinculo.cnpj,
                        concat('2|', COALESCE(termo.numero_contrato, ''), '|', COALESCE(termo.numero_termo, '')),
                        to_json(struct_pack(
                            tipo_registro := 'termo_aditivo',
                            numero := COALESCE(termo.numero_contrato, ''),
                            numero_termo := COALESCE(termo.numero_termo, ''),
                            data_publicacao := COALESCE(termo.data_publicacao, ''),
                            objeto := COALESCE(termo.objeto, ''),
                            orgao_codigo := COALESCE(termo.codigo_orgao, ''),
                            orgao_nome := COALESCE(termo.nome_orgao, ''),
                            ug_codigo := COALESCE(termo.codigo_ug, ''),
                            ug_nome := COALESCE(termo.nome_ug, '')
                        ))
                    FROM termos termo
                    INNER JOIN vinculos vinculo USING (
                        numero_contrato,
                        codigo_orgao,
                        codigo_ug)

                    UNION ALL

                    SELECT
                        vinculo.cnpj,
                        concat('3|', COALESCE(apostilamento.numero_contrato, ''), '|', COALESCE(apostilamento.numero_apostilamento, '')),
                        to_json(struct_pack(
                            tipo_registro := 'apostilamento',
                            numero := COALESCE(apostilamento.numero_contrato, ''),
                            numero_apostilamento := COALESCE(apostilamento.numero_apostilamento, ''),
                            descricao := COALESCE(apostilamento.descricao, ''),
                            valor := COALESCE(apostilamento.valor, ''),
                            data_inclusao := COALESCE(apostilamento.data_inclusao, ''),
                            situacao := COALESCE(apostilamento.situacao, ''),
                            orgao_codigo := COALESCE(apostilamento.codigo_orgao, ''),
                            orgao_nome := COALESCE(apostilamento.nome_orgao, ''),
                            ug_codigo := COALESCE(apostilamento.codigo_ug, ''),
                            ug_nome := COALESCE(apostilamento.nome_ug, '')
                        ))
                    FROM apostilamentos apostilamento
                    INNER JOIN vinculos vinculo USING (
                        numero_contrato,
                        codigo_orgao,
                        codigo_ug)
                )
                SELECT cnpj, record_sort, record_json
                FROM eventos
                """;
    }

    private static string BuildRenunciasQuery(ExtractedDataset files)
    {
        var renuncias = Csv(
            files.RequireFilesEnding("_RenúnciasFiscais.csv"),
            "ano",
            "cnpj",
            "razao_social",
            "nome_fantasia",
            "codigo_cnae",
            "cnae",
            "municipio",
            "uf",
            "tipo_renuncia",
            "beneficio",
            "fundamento_legal",
            "descricao",
            "tributo",
            "forma_tributacao",
            "valor");
        var habilitadas = Csv(
            files.RequireFilesEnding("_EmpresasHabilitadas.csv"),
            "cnpj",
            "razao_social",
            "nome_fantasia",
            "codigo_cnae",
            "cnae",
            "municipio",
            "uf",
            "beneficio",
            "base_legal",
            "descricao",
            "inicio",
            "fim");
        var imunes = Csv(
            files.RequireFilesEnding("_EmpresasImunesOuIsentas.csv"),
            "ano",
            "cnpj",
            "razao_social",
            "nome_fantasia",
            "codigo_cnae",
            "cnae",
            "municipio",
            "uf",
            "tipo_entidade",
            "beneficio");
        var beneficiarios = Csv(
            files.RequireFilesEnding("_RenúnciasFiscaisPorBeneficiário.csv"),
            "ano",
            "cnpj",
            "razao_social",
            "nome_fantasia",
            "codigo_cnae",
            "cnae",
            "municipio",
            "uf",
            "valor");

        return $"""
                SELECT
                    CleanCnpj(cnpj) AS cnpj,
                    concat('0|', COALESCE(ano, ''), '|', COALESCE(beneficio, ''), '|', COALESCE(tributo, '')) AS record_sort,
                    to_json(struct_pack(
                        tipo_registro := 'renuncia_fiscal',
                        ano_calendario := COALESCE(ano, ''),
                        razao_social := COALESCE(razao_social, ''),
                        nome_fantasia := COALESCE(nome_fantasia, ''),
                        cnae_codigo := COALESCE(codigo_cnae, ''),
                        cnae := COALESCE(cnae, ''),
                        municipio := COALESCE(municipio, ''),
                        uf := COALESCE(uf, ''),
                        tipo_renuncia := COALESCE(tipo_renuncia, ''),
                        beneficio_fiscal := COALESCE(beneficio, ''),
                        fundamento_legal := COALESCE(fundamento_legal, ''),
                        descricao := COALESCE(descricao, ''),
                        tributo := COALESCE(tributo, ''),
                        forma_tributacao := COALESCE(forma_tributacao, ''),
                        valor := COALESCE(valor, '')
                    )) AS record_json
                FROM {renuncias}
                WHERE {Valid("CleanCnpj(cnpj)")}

                UNION ALL

                SELECT
                    CleanCnpj(cnpj),
                    concat('1|', COALESCE(beneficio, ''), '|', COALESCE(inicio, '')),
                    to_json(struct_pack(
                        tipo_registro := 'empresa_habilitada',
                        razao_social := COALESCE(razao_social, ''),
                        nome_fantasia := COALESCE(nome_fantasia, ''),
                        cnae_codigo := COALESCE(codigo_cnae, ''),
                        cnae := COALESCE(cnae, ''),
                        municipio := COALESCE(municipio, ''),
                        uf := COALESCE(uf, ''),
                        beneficio_fiscal := COALESCE(beneficio, ''),
                        fundamento_legal := COALESCE(base_legal, ''),
                        descricao := COALESCE(descricao, ''),
                        inicio_habilitacao := COALESCE(inicio, ''),
                        fim_habilitacao := COALESCE(fim, '')
                    ))
                FROM {habilitadas}
                WHERE {Valid("CleanCnpj(cnpj)")}

                UNION ALL

                SELECT
                    CleanCnpj(cnpj),
                    concat('2|', COALESCE(ano, ''), '|', COALESCE(beneficio, '')),
                    to_json(struct_pack(
                        tipo_registro := 'empresa_imune_ou_isenta',
                        ano_calendario := COALESCE(ano, ''),
                        razao_social := COALESCE(razao_social, ''),
                        nome_fantasia := COALESCE(nome_fantasia, ''),
                        cnae_codigo := COALESCE(codigo_cnae, ''),
                        cnae := COALESCE(cnae, ''),
                        municipio := COALESCE(municipio, ''),
                        uf := COALESCE(uf, ''),
                        tipo_entidade := COALESCE(tipo_entidade, ''),
                        beneficio_fiscal := COALESCE(beneficio, '')
                    ))
                FROM {imunes}
                WHERE {Valid("CleanCnpj(cnpj)")}

                UNION ALL

                SELECT
                    CleanCnpj(cnpj),
                    concat('3|', COALESCE(ano, ''), '|', COALESCE(valor, '')),
                    to_json(struct_pack(
                        tipo_registro := 'total_por_beneficiario',
                        ano_calendario := COALESCE(ano, ''),
                        razao_social := COALESCE(razao_social, ''),
                        nome_fantasia := COALESCE(nome_fantasia, ''),
                        cnae_codigo := COALESCE(codigo_cnae, ''),
                        cnae := COALESCE(cnae, ''),
                        municipio := COALESCE(municipio, ''),
                        uf := COALESCE(uf, ''),
                        valor := COALESCE(valor, '')
                    ))
                FROM {beneficiarios}
                WHERE {Valid("CleanCnpj(cnpj)")}
                """;
    }

    private static string BuildNotasFiscaisQuery(ExtractedDataset files)
    {
        var notas = Csv(
            files.RequireFilesEnding("_NFe_NotaFiscal.csv"),
            "chave",
            "modelo",
            "serie",
            "numero",
            "natureza_operacao",
            "data_emissao",
            "evento_recente",
            "data_evento_recente",
            "cnpj_emitente",
            "razao_social_emitente",
            "inscricao_estadual_emitente",
            "uf_emitente",
            "municipio_emitente",
            "codigo_orgao_superior_destinatario",
            "orgao_superior_destinatario",
            "codigo_orgao_destinatario",
            "orgao_destinatario",
            "cnpj_destinatario",
            "nome_destinatario",
            "uf_destinatario",
            "indicador_ie_destinatario",
            "destino_operacao",
            "consumidor_final",
            "presenca_comprador",
            "valor");
        var itens = Csv(
            files.RequireFilesEnding("_NFe_NotaFiscalItem.csv"),
            "chave",
            "modelo",
            "serie",
            "numero",
            "natureza_operacao",
            "data_emissao",
            "cnpj_emitente",
            "razao_social_emitente",
            "inscricao_estadual_emitente",
            "uf_emitente",
            "municipio_emitente",
            "codigo_orgao_superior_destinatario",
            "orgao_superior_destinatario",
            "codigo_orgao_destinatario",
            "orgao_destinatario",
            "cnpj_destinatario",
            "nome_destinatario",
            "uf_destinatario",
            "indicador_ie_destinatario",
            "destino_operacao",
            "consumidor_final",
            "presenca_comprador",
            "numero_produto",
            "descricao_produto",
            "codigo_ncm",
            "tipo_ncm",
            "cfop",
            "quantidade",
            "unidade",
            "valor_unitario",
            "valor_total");
        var eventos = Csv(
            files.RequireFilesEnding("_NFe_NotaFiscalEvento.csv"),
            "chave",
            "modelo",
            "serie",
            "numero",
            "natureza_operacao",
            "data_emissao",
            "evento",
            "data_evento",
            "descricao_evento",
            "motivo_evento");

        return $"""
                WITH
                notas AS (SELECT * FROM {notas}),
                itens AS (SELECT * FROM {itens}),
                eventos_nfe AS (SELECT * FROM {eventos}),
                vinculos AS (
                    SELECT
                        CleanCnpj(cnpj_emitente) AS cnpj,
                        'emitente' AS papel,
                        chave
                    FROM notas
                    WHERE {Valid("CleanCnpj(cnpj_emitente)")}
                    UNION ALL
                    SELECT
                        CleanCnpj(cnpj_destinatario),
                        'destinatario',
                        chave
                    FROM notas
                    WHERE {Valid("CleanCnpj(cnpj_destinatario)")}
                ),
                itens_vinculados AS (
                    SELECT
                        CleanCnpj(cnpj_emitente) AS cnpj,
                        'emitente' AS papel,
                        itens.*
                    FROM itens
                    WHERE {Valid("CleanCnpj(cnpj_emitente)")}
                    UNION ALL
                    SELECT
                        CleanCnpj(cnpj_destinatario),
                        'destinatario',
                        itens.*
                    FROM itens
                    WHERE {Valid("CleanCnpj(cnpj_destinatario)")}
                ),
                registros AS (
                    SELECT
                        vinculo.cnpj,
                        concat('0|', COALESCE(nota.chave, ''), '|', vinculo.papel) AS record_sort,
                        to_json(struct_pack(
                            tipo_registro := 'nota_fiscal',
                            papel_cnpj := vinculo.papel,
                            chave_acesso := COALESCE(nota.chave, ''),
                            modelo := COALESCE(nota.modelo, ''),
                            serie := COALESCE(nota.serie, ''),
                            numero := COALESCE(nota.numero, ''),
                            natureza_operacao := COALESCE(nota.natureza_operacao, ''),
                            data_emissao := COALESCE(nota.data_emissao, ''),
                            evento_mais_recente := COALESCE(nota.evento_recente, ''),
                            data_evento_mais_recente := COALESCE(nota.data_evento_recente, ''),
                            razao_social_emitente := COALESCE(nota.razao_social_emitente, ''),
                            inscricao_estadual_emitente := COALESCE(nota.inscricao_estadual_emitente, ''),
                            uf_emitente := COALESCE(nota.uf_emitente, ''),
                            municipio_emitente := COALESCE(nota.municipio_emitente, ''),
                            orgao_superior_destinatario_codigo := COALESCE(nota.codigo_orgao_superior_destinatario, ''),
                            orgao_superior_destinatario := COALESCE(nota.orgao_superior_destinatario, ''),
                            orgao_destinatario_codigo := COALESCE(nota.codigo_orgao_destinatario, ''),
                            orgao_destinatario := COALESCE(nota.orgao_destinatario, ''),
                            nome_destinatario := COALESCE(nota.nome_destinatario, ''),
                            uf_destinatario := COALESCE(nota.uf_destinatario, ''),
                            destino_operacao := COALESCE(nota.destino_operacao, ''),
                            consumidor_final := COALESCE(nota.consumidor_final, ''),
                            presenca_comprador := COALESCE(nota.presenca_comprador, ''),
                            valor := COALESCE(nota.valor, '')
                        )) AS record_json
                    FROM notas nota
                    INNER JOIN vinculos vinculo USING (chave)

                    UNION ALL

                    SELECT
                        item.cnpj,
                        concat('1|', COALESCE(item.chave, ''), '|', item.papel, '|', COALESCE(item.numero_produto, '')),
                        to_json(struct_pack(
                            tipo_registro := 'item',
                            papel_cnpj := item.papel,
                            chave_acesso := COALESCE(item.chave, ''),
                            numero_produto := COALESCE(item.numero_produto, ''),
                            descricao := COALESCE(item.descricao_produto, ''),
                            ncm_codigo := COALESCE(item.codigo_ncm, ''),
                            ncm_tipo := COALESCE(item.tipo_ncm, ''),
                            cfop := COALESCE(item.cfop, ''),
                            quantidade := COALESCE(item.quantidade, ''),
                            unidade := COALESCE(item.unidade, ''),
                            valor_unitario := COALESCE(item.valor_unitario, ''),
                            valor_total := COALESCE(item.valor_total, '')
                        ))
                    FROM itens_vinculados item

                    UNION ALL

                    SELECT
                        vinculo.cnpj,
                        concat('2|', COALESCE(evento.chave, ''), '|', vinculo.papel, '|', COALESCE(evento.evento, ''), '|', COALESCE(evento.data_evento, '')),
                        to_json(struct_pack(
                            tipo_registro := 'evento',
                            papel_cnpj := vinculo.papel,
                            chave_acesso := COALESCE(evento.chave, ''),
                            evento := COALESCE(evento.evento, ''),
                            data_evento := COALESCE(evento.data_evento, ''),
                            descricao := COALESCE(evento.descricao_evento, ''),
                            motivo := COALESCE(evento.motivo_evento, '')
                        ))
                    FROM eventos_nfe evento
                    INNER JOIN vinculos vinculo USING (chave)
                )
                SELECT
                    cnpj,
                    record_sort,
                    record_json
                FROM registros
                """;
    }

    private static string BuildConveniosQuery(ExtractedDataset files)
    {
        var convenios = Csv(
            files.RequireFilesEnding("_Convenios.csv"),
            "numero_convenio",
            "uf",
            "codigo_municipio",
            "municipio",
            "situacao",
            "numero_original",
            "numero_processo",
            "objeto",
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao_concedente",
            "nome_orgao_concedente",
            "codigo_ug_concedente",
            "nome_ug_concedente",
            "codigo_convenente",
            "tipo_convenente",
            "nome_convenente",
            "tipo_ente_convenente",
            "tipo_instrumento",
            "valor",
            "valor_liberado",
            "data_publicacao",
            "data_inicio",
            "data_fim",
            "valor_contrapartida",
            "data_ultima_liberacao",
            "valor_ultima_liberacao");
        var ordens = Csv(
            files.RequireFilesEnding("_Convenios_OrdensBancarias.csv"),
            "numero_convenio",
            "numero_original",
            "data_emissao",
            "numero_ordem",
            "valor_liberado");

        return $"""
                WITH
                convenios AS (SELECT * FROM {convenios}),
                ordens AS (SELECT * FROM {ordens}),
                vinculos AS (
                    SELECT DISTINCT
                        CleanCnpj(codigo_convenente) AS cnpj,
                        numero_convenio
                    FROM convenios
                    WHERE {Valid("CleanCnpj(codigo_convenente)")}
                )
                SELECT
                    CleanCnpj(codigo_convenente) AS cnpj,
                    concat('0|', COALESCE(numero_convenio, '')) AS record_sort,
                    to_json(struct_pack(
                        tipo_registro := 'convenio',
                        numero := COALESCE(numero_convenio, ''),
                        numero_original := COALESCE(numero_original, ''),
                        numero_processo := COALESCE(numero_processo, ''),
                        objeto := COALESCE(objeto, ''),
                        situacao := COALESCE(situacao, ''),
                        tipo_instrumento := COALESCE(tipo_instrumento, ''),
                        tipo_convenente := COALESCE(tipo_convenente, ''),
                        tipo_ente_convenente := COALESCE(tipo_ente_convenente, ''),
                        nome_convenente := COALESCE(nome_convenente, ''),
                        municipio_codigo := COALESCE(codigo_municipio, ''),
                        municipio := COALESCE(municipio, ''),
                        uf := COALESCE(uf, ''),
                        orgao_superior_codigo := COALESCE(codigo_orgao_superior, ''),
                        orgao_superior_nome := COALESCE(nome_orgao_superior, ''),
                        orgao_concedente_codigo := COALESCE(codigo_orgao_concedente, ''),
                        orgao_concedente_nome := COALESCE(nome_orgao_concedente, ''),
                        ug_concedente_codigo := COALESCE(codigo_ug_concedente, ''),
                        ug_concedente_nome := COALESCE(nome_ug_concedente, ''),
                        valor := COALESCE(valor, ''),
                        valor_liberado := COALESCE(valor_liberado, ''),
                        valor_contrapartida := COALESCE(valor_contrapartida, ''),
                        data_publicacao := COALESCE(data_publicacao, ''),
                        data_inicio := COALESCE(data_inicio, ''),
                        data_fim := COALESCE(data_fim, ''),
                        data_ultima_liberacao := COALESCE(data_ultima_liberacao, ''),
                        valor_ultima_liberacao := COALESCE(valor_ultima_liberacao, '')
                    )) AS record_json
                FROM convenios
                WHERE {Valid("CleanCnpj(codigo_convenente)")}

                UNION ALL

                SELECT
                    vinculo.cnpj,
                    concat('1|', COALESCE(ordem.numero_convenio, ''), '|', COALESCE(ordem.numero_ordem, '')),
                    to_json(struct_pack(
                        tipo_registro := 'ordem_bancaria',
                        numero_convenio := COALESCE(ordem.numero_convenio, ''),
                        numero_original := COALESCE(ordem.numero_original, ''),
                        numero_ordem_bancaria := COALESCE(ordem.numero_ordem, ''),
                        data_emissao := COALESCE(ordem.data_emissao, ''),
                        valor_liberado := COALESCE(ordem.valor_liberado, '')
                    ))
                FROM ordens ordem
                INNER JOIN vinculos vinculo USING (numero_convenio)
                """;
    }

    private static IReadOnlyList<string> BuildEmendasQueries(
        ExtractedDataset files)
    {
        var favorecidos = Csv(
            files.RequireFilesEnding("EmendasParlamentares_PorFavorecido.csv"),
            "codigo_emenda",
            "codigo_autor",
            "nome_autor",
            "numero",
            "tipo",
            "ano_mes",
            "codigo_favorecido",
            "favorecido",
            "natureza_juridica",
            "tipo_favorecido",
            "uf_favorecido",
            "municipio_favorecido",
            "valor_recebido");

        var favorecimentosQuery = $"""
                WITH
                favorecidos AS (SELECT * FROM {favorecidos})
                SELECT
                    CleanCnpj(favorecido.codigo_favorecido) AS cnpj,
                    concat('1|', COALESCE(favorecido.codigo_emenda, ''), '|', COALESCE(favorecido.ano_mes, ''), '|', COALESCE(favorecido.valor_recebido, '')) AS record_sort,
                    to_json(struct_pack(
                        tipo_registro := 'favorecimento',
                        codigo_emenda := COALESCE(favorecido.codigo_emenda, ''),
                        ano_mes := COALESCE(favorecido.ano_mes, ''),
                        numero_emenda := COALESCE(favorecido.numero, ''),
                        tipo_emenda := COALESCE(favorecido.tipo, ''),
                        autor_codigo := COALESCE(favorecido.codigo_autor, ''),
                        autor_nome := COALESCE(favorecido.nome_autor, ''),
                        favorecido := COALESCE(favorecido.favorecido, ''),
                        natureza_juridica := COALESCE(favorecido.natureza_juridica, ''),
                        tipo_favorecido := COALESCE(favorecido.tipo_favorecido, ''),
                        uf_favorecido := COALESCE(favorecido.uf_favorecido, ''),
                        municipio_favorecido := COALESCE(favorecido.municipio_favorecido, ''),
                        valor_recebido := COALESCE(favorecido.valor_recebido, '')
                    )) AS record_json
                FROM favorecidos favorecido
                WHERE {Valid("CleanCnpj(favorecido.codigo_favorecido)")}
                """;
        return [favorecimentosQuery];
    }

    private static string BuildEmendasDocumentosQuery(ExtractedDataset files)
    {
        var documentos = Csv(
            files.RequireFilesEnding("_EmendasParlamentares_PorDocumento.csv"),
            "codigo_emenda",
            "ano",
            "codigo_autor",
            "nome_autor",
            "numero_emenda",
            "valor_empenhado",
            "valor_pago",
            "tipo_emenda",
            "data_documento",
            "codigo_documento",
            "localidade",
            "uf_aplicacao",
            "municipio_aplicacao",
            "codigo_ibge",
            "fase_despesa",
            "codigo_favorecido",
            "favorecido",
            "tipo_favorecido",
            "uf_favorecido",
            "municipio_favorecido",
            "codigo_ug",
            "ug",
            "codigo_unidade_orcamentaria",
            "unidade_orcamentaria",
            "codigo_orgao",
            "orgao",
            "codigo_orgao_superior",
            "orgao_superior",
            "codigo_grupo_despesa",
            "grupo_despesa",
            "codigo_elemento_despesa",
            "elemento_despesa",
            "codigo_modalidade_aplicacao",
            "modalidade_aplicacao",
            "codigo_plano",
            "plano",
            "codigo_funcao",
            "funcao",
            "codigo_subfuncao",
            "subfuncao",
            "codigo_programa",
            "programa",
            "codigo_acao",
            "acao",
            "linguagem_cidada",
            "codigo_subtitulo",
            "subtitulo",
            "possui_convenio");

        return $"""
                SELECT
                    CleanCnpj(codigo_favorecido) AS cnpj,
                    concat(
                        COALESCE(data_documento, ''),
                        '|',
                        COALESCE(codigo_documento, ''),
                        '|',
                        COALESCE(fase_despesa, '')) AS record_sort,
                    to_json(struct_pack(
                        codigo_emenda := COALESCE(codigo_emenda, ''),
                        ano_emenda := COALESCE(ano, ''),
                        autor_codigo := COALESCE(codigo_autor, ''),
                        autor_nome := COALESCE(nome_autor, ''),
                        numero_emenda := COALESCE(numero_emenda, ''),
                        tipo_emenda := COALESCE(tipo_emenda, ''),
                        data_documento := COALESCE(data_documento, ''),
                        codigo_documento := COALESCE(codigo_documento, ''),
                        fase_despesa := COALESCE(fase_despesa, ''),
                        valor_empenhado := COALESCE(valor_empenhado, ''),
                        valor_pago := COALESCE(valor_pago, ''),
                        favorecido := COALESCE(favorecido, ''),
                        tipo_favorecido := COALESCE(tipo_favorecido, ''),
                        uf_favorecido := COALESCE(uf_favorecido, ''),
                        municipio_favorecido := COALESCE(municipio_favorecido, ''),
                        localidade_aplicacao := COALESCE(localidade, ''),
                        uf_aplicacao := COALESCE(uf_aplicacao, ''),
                        municipio_aplicacao := COALESCE(municipio_aplicacao, ''),
                        codigo_ibge_aplicacao := COALESCE(codigo_ibge, ''),
                        ug_codigo := COALESCE(codigo_ug, ''),
                        ug := COALESCE(ug, ''),
                        unidade_orcamentaria_codigo := COALESCE(codigo_unidade_orcamentaria, ''),
                        unidade_orcamentaria := COALESCE(unidade_orcamentaria, ''),
                        orgao_codigo := COALESCE(codigo_orgao, ''),
                        orgao := COALESCE(orgao, ''),
                        orgao_superior_codigo := COALESCE(codigo_orgao_superior, ''),
                        orgao_superior := COALESCE(orgao_superior, ''),
                        grupo_despesa_codigo := COALESCE(codigo_grupo_despesa, ''),
                        grupo_despesa := COALESCE(grupo_despesa, ''),
                        elemento_despesa_codigo := COALESCE(codigo_elemento_despesa, ''),
                        elemento_despesa := COALESCE(elemento_despesa, ''),
                        modalidade_aplicacao_codigo := COALESCE(codigo_modalidade_aplicacao, ''),
                        modalidade_aplicacao := COALESCE(modalidade_aplicacao, ''),
                        plano_orcamentario_codigo := COALESCE(codigo_plano, ''),
                        plano_orcamentario := COALESCE(plano, ''),
                        funcao_codigo := COALESCE(codigo_funcao, ''),
                        funcao := COALESCE(funcao, ''),
                        subfuncao_codigo := COALESCE(codigo_subfuncao, ''),
                        subfuncao := COALESCE(subfuncao, ''),
                        programa_codigo := COALESCE(codigo_programa, ''),
                        programa := COALESCE(programa, ''),
                        acao_codigo := COALESCE(codigo_acao, ''),
                        acao := COALESCE(acao, ''),
                        linguagem_cidada := COALESCE(linguagem_cidada, ''),
                        subtitulo_codigo := COALESCE(codigo_subtitulo, ''),
                        subtitulo := COALESCE(subtitulo, ''),
                        possui_convenio := COALESCE(possui_convenio, '')
                    )) AS record_json
                FROM {documentos}
                WHERE {Valid("CleanCnpj(codigo_favorecido)")}
                """;
    }

    private static string Csv(
        IReadOnlyList<string> paths,
        params string[] columns)
    {
        var pathList = string.Join(
            ", ",
            paths.Select(path => $"'{Sql(path)}'"));
        var columnMap = string.Join(
            ", ",
            columns.Select(column => $"'{column}': 'VARCHAR'"));
        var columnsSql = "{" + columnMap + "}";
        return $"""
                read_csv(
                    [{pathList}],
                    columns = {columnsSql},
                    header = true,
                    auto_detect = false,
                    all_varchar = true,
                    delim = ';',
                    quote = '"',
                    escape = '"',
                    nullstr = '',
                    strict_mode = true,
                    ignore_errors = false,
                    parallel = true
                )
                """;
    }

    private static string Valid(string expression) =>
        $"regexp_full_match({expression}, '[A-Z0-9]{{12}}[0-9]{{2}}')";

    private static string Sql(string value) =>
        ParquetProcessor.EscapeSqlLiteral(value);

    private static void DeleteFile(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static void DeleteDirectory(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }

    private static string GetCompletionMarker(string outputDirectory) =>
        Path.Combine(outputDirectory, ".complete");
}
