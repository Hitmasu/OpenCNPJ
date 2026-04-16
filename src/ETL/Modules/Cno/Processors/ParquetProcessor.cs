using System.IO.Compression;
using System.Text;
using CNPJExporter.Modules.Cno.Models;
using DuckDB.NET.Data;

namespace CNPJExporter.Modules.Cno.Processors;

public sealed class ParquetProcessor
{
    private static readonly IReadOnlyDictionary<string, string> RequiredEntries = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        ["cno.csv"] = "Obras",
        ["cno_cnaes.csv"] = "Cnaes",
        ["cno_vinculos.csv"] = "Vinculos",
        ["cno_areas.csv"] = "Areas"
    };

    public async Task<ExtractedFiles> ExtractAsync(
        string zipPath,
        string extractDir,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(extractDir);

        using var archive = ZipFile.OpenRead(zipPath);
        var extractedPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var (entryName, _) in RequiredEntries)
        {
            var entry = archive.Entries.FirstOrDefault(e => string.Equals(Path.GetFileName(e.FullName), entryName, StringComparison.OrdinalIgnoreCase))
                ?? throw new InvalidOperationException($"Arquivo {entryName} não encontrado dentro do ZIP do CNO.");

            var outputPath = Path.Combine(extractDir, entryName);
            if (!File.Exists(outputPath) || new FileInfo(outputPath).Length != entry.Length)
            {
                await using var input = entry.Open();
                await using var output = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.Read, 1 << 20, useAsync: true);
                await input.CopyToAsync(output, cancellationToken);
            }

            extractedPaths[entryName] = outputPath;
        }

        return new ExtractedFiles(
            extractedPaths["cno.csv"],
            extractedPaths["cno_cnaes.csv"],
            extractedPaths["cno_vinculos.csv"],
            extractedPaths["cno_areas.csv"]);
    }

    public async Task ConvertToParquetAsync(
        ExtractedFiles files,
        string parquetPath,
        DateTimeOffset moduleUpdatedAt,
        int shardPrefixLength,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(parquetPath)!);
        DeleteIfExists(parquetPath);
        var utf8Files = await ConvertInputsToUtf8Async(files, cancellationToken);
        var workDir = Path.GetDirectoryName(parquetPath)!;
        var duckDbPath = Path.Combine(workDir, "cno-import.duckdb");
        DeleteIfExists(duckDbPath);
        DeleteIfExists(duckDbPath + ".wal");

        await using (var connection = new DuckDBConnection($"Data Source={duckDbPath}"))
        {
            await connection.OpenAsync();

            await ConfigureDuckDbAsync(connection, workDir, cancellationToken);

            await ExecuteNonQueryAsync(
                connection,
                "CREATE OR REPLACE MACRO CleanCnpj(value) AS regexp_replace(COALESCE(CAST(value AS VARCHAR), ''), '[^0-9A-Za-z]', '', 'g')",
                cancellationToken);
            await ExecuteNonQueryAsync(
                connection,
                BuildImportSql(utf8Files, shardPrefixLength),
                cancellationToken);

            await ExecuteNonQueryAsync(
                connection,
                BuildInsertAllSql(moduleUpdatedAt),
                cancellationToken);

            await ExecuteNonQueryAsync(
                connection,
                $"COPY cno_output TO '{EscapeSqlLiteral(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)",
                cancellationToken);
        }

        DeleteIfExists(duckDbPath);
        DeleteIfExists(duckDbPath + ".wal");
    }

    public async Task<Dictionary<string, string>> LoadHashesAsync(
        string parquetPath,
        CancellationToken cancellationToken = default)
    {
        var hashes = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT cnpj, content_hash
            FROM read_parquet('{EscapeSqlLiteral(parquetPath)}')
            WHERE cnpj IS NOT NULL AND content_hash IS NOT NULL";

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            hashes[reader.GetString(0)] = reader.GetString(1);
        }

        return hashes;
    }

    private static string BuildImportSql(
        ExtractedFiles files,
        int shardPrefixLength)
    {
        var obras = BuildCsvRead(files.ObrasPath, [
            "cno", "codigo_pais", "nome_pais", "data_inicio", "data_inicio_responsabilidade",
            "data_registro", "cno_vinculado", "cep", "ni_responsavel", "qualificacao_responsavel",
            "nome", "codigo_municipio", "nome_municipio", "tipo_logradouro", "logradouro",
            "numero_logradouro", "bairro", "estado", "caixa_postal", "complemento",
            "unidade_medida", "area_total", "situacao", "data_situacao", "nome_empresarial",
            "codigo_localizacao"
        ]);
        var vinculos = BuildCsvRead(files.VinculosPath, [
            "cno", "data_inicio", "data_fim", "data_registro", "qualificacao_contribuinte", "ni_responsavel"
        ]);

        return $@"
            CREATE TABLE obras AS SELECT * FROM {obras};
            CREATE TABLE vinculos AS SELECT * FROM {vinculos};
            CREATE TABLE owners AS
                SELECT DISTINCT cnpj, substring(cnpj, 1, {shardPrefixLength}) AS cnpj_prefix, cno
                FROM (
                    SELECT CleanCnpj(ni_responsavel) AS cnpj, cno
                    FROM obras
                    WHERE length(CleanCnpj(ni_responsavel)) = 14
                    UNION
                    SELECT CleanCnpj(ni_responsavel) AS cnpj, cno
                    FROM vinculos
                    WHERE length(CleanCnpj(ni_responsavel)) = 14
                ) AS normalized_owners;
            CREATE TABLE cno_output (
                cnpj VARCHAR,
                cnpj_prefix VARCHAR,
                payload_json VARCHAR,
                content_hash VARCHAR,
                source_updated_at VARCHAR,
                module_updated_at VARCHAR
            );";
    }

    private static string BuildInsertAllSql(DateTimeOffset moduleUpdatedAt)
    {
        var updatedAt = EscapeSqlLiteral(moduleUpdatedAt.ToString("O"));

        return $@"
            INSERT INTO cno_output
            WITH
                obra_payloads AS (
                    SELECT
                        owners.cnpj,
                        owners.cnpj_prefix,
                        obras.cno,
                        struct_pack(
                            cno := COALESCE(obras.cno, ''),
                            nome := COALESCE(obras.nome, ''),
                            nome_empresarial := COALESCE(obras.nome_empresarial, ''),
                            situacao := {BuildSituationStructSql("obras.situacao")},
                            data_inicio := COALESCE(obras.data_inicio, ''),
                            data_inicio_responsabilidade := COALESCE(obras.data_inicio_responsabilidade, ''),
                            data_registro := COALESCE(obras.data_registro, ''),
                            data_situacao := COALESCE(obras.data_situacao, ''),
                            cep := COALESCE(obras.cep, ''),
                            uf := COALESCE(obras.estado, ''),
                            codigo_municipio := COALESCE(obras.codigo_municipio, ''),
                            municipio := COALESCE(obras.nome_municipio, ''),
                            tipo_logradouro := COALESCE(obras.tipo_logradouro, ''),
                            logradouro := COALESCE(obras.logradouro, ''),
                            numero := COALESCE(obras.numero_logradouro, ''),
                            bairro := COALESCE(obras.bairro, ''),
                            complemento := COALESCE(obras.complemento, ''),
                            unidade_medida := COALESCE(obras.unidade_medida, ''),
                            area_total := COALESCE(obras.area_total, ''),
                            cno_vinculado := COALESCE(obras.cno_vinculado, ''),
                            codigo_pais := COALESCE(obras.codigo_pais, ''),
                            pais := COALESCE(obras.nome_pais, ''),
                            qualificacao_responsavel := {BuildQualificationStructSql("obras.qualificacao_responsavel")},
                            codigo_localizacao := COALESCE(obras.codigo_localizacao, '')
                        ) AS obra
                    FROM owners
                    INNER JOIN obras ON obras.cno = owners.cno
                ),
                payloads AS (
                    SELECT
                        cnpj,
                        cnpj_prefix,
                        to_json(struct_pack(
                            updated_at := '{updatedAt}',
                            obras := array_agg(obra)
                        )) AS payload_json
                    FROM obra_payloads
                    GROUP BY cnpj, cnpj_prefix
                )
                SELECT
                    cnpj,
                    cnpj_prefix,
                    payload_json,
                    md5(payload_json) AS content_hash,
                    '{updatedAt}' AS source_updated_at,
                    '{updatedAt}' AS module_updated_at
                FROM payloads";
    }

    private static string BuildSituationStructSql(string column) =>
        $@"struct_pack(
            codigo := COALESCE({column}, ''),
            descricao := CASE COALESCE({column}, '')
                WHEN '01' THEN 'NULA'
                WHEN '02' THEN 'ATIVA'
                WHEN '03' THEN 'SUSPENSA'
                WHEN '14' THEN 'PARALISADA'
                WHEN '15' THEN 'ENCERRADA'
                ELSE ''
            END
        )";

    private static string BuildQualificationStructSql(string column) =>
        $@"struct_pack(
            codigo := COALESCE({column}, ''),
            descricao := CASE COALESCE({column}, '')
                WHEN '0070' THEN 'Proprietário do Imóvel'
                WHEN '0057' THEN 'Dono da Obra'
                WHEN '0064' THEN 'Incorporador de Construção Civil'
                WHEN '0053' THEN 'Pessoa Jurídica Construtora'
                WHEN '0111' THEN 'Sociedade Líder de Consórcio'
                WHEN '0109' THEN 'Consórcio'
                WHEN '0110' THEN 'Construção em nome coletivo'
                ELSE ''
            END
        )";

    private static string BuildCsvRead(string path, IReadOnlyList<string> columns)
    {
        var columnMap = string.Join(", ", columns.Select(column => $"'{column}': 'VARCHAR'"));
        return $@"
            read_csv(
                '{EscapeSqlLiteral(path)}',
                columns = {{{columnMap}}},
                header = true,
                auto_detect = false,
                all_varchar = true,
                delim = ',',
                quote = '""',
                escape = '""',
                nullstr = '',
                ignore_errors = true
            )";
    }

    private static async Task<ExtractedFiles> ConvertInputsToUtf8Async(
        ExtractedFiles files,
        CancellationToken cancellationToken)
    {
        var outputDir = Path.Combine(Path.GetDirectoryName(files.ObrasPath)!, "_utf8");
        Directory.CreateDirectory(outputDir);

        return new ExtractedFiles(
            await ConvertLatin1FileToUtf8Async(files.ObrasPath, Path.Combine(outputDir, "cno.csv"), cancellationToken),
            files.CnaesPath,
            await ConvertLatin1FileToUtf8Async(files.VinculosPath, Path.Combine(outputDir, "cno_vinculos.csv"), cancellationToken),
            files.AreasPath);
    }

    private static async Task<string> ConvertLatin1FileToUtf8Async(
        string sourcePath,
        string destinationPath,
        CancellationToken cancellationToken)
    {
        if (File.Exists(destinationPath)
            && File.GetLastWriteTimeUtc(destinationPath) >= File.GetLastWriteTimeUtc(sourcePath))
        {
            return destinationPath;
        }

        await using var input = new FileStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, useAsync: true);
        await using var output = new FileStream(destinationPath, FileMode.Create, FileAccess.Write, FileShare.Read, 1 << 20, useAsync: true);
        using var reader = new StreamReader(input, Encoding.Latin1, detectEncodingFromByteOrderMarks: false, bufferSize: 1 << 20, leaveOpen: true);
        await using var writer = new StreamWriter(output, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1 << 20, leaveOpen: true);

        var buffer = new char[1 << 20];
        int read;
        while ((read = await reader.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken)) > 0)
        {
            await writer.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
        }

        return destinationPath;
    }

    private static async Task ConfigureDuckDbAsync(
        DuckDBConnection connection,
        string workDir,
        CancellationToken cancellationToken)
    {
        var tempDir = Path.Combine(workDir, "_duckdb_temp");
        Directory.CreateDirectory(tempDir);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SET preserve_insertion_order = false;
            SET threads = 1;
            SET memory_limit = '8GB';
            SET temp_directory = '{EscapeSqlLiteral(tempDir)}';
            SET max_temp_directory_size = '200GB';";
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task ExecuteNonQueryAsync(
        DuckDBConnection connection,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
