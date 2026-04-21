using System.Text;
using DuckDB.NET.Data;

namespace CNPJExporter.Modules.Rntrc.Processors;

public sealed class ParquetProcessor
{
    public async Task ConvertToParquetAsync(
        string csvPath,
        string parquetPath,
        DateTimeOffset moduleUpdatedAt,
        int shardPrefixLength,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(parquetPath)!);
        DeleteIfExists(parquetPath);
        var utf8CsvPath = await ConvertLatin1FileToUtf8Async(
            csvPath,
            Path.Combine(Path.GetDirectoryName(csvPath)!, "_utf8", Path.GetFileName(csvPath)),
            cancellationToken);
        var workDir = Path.GetDirectoryName(parquetPath)!;
        var duckDbPath = Path.Combine(workDir, "rntrc-import.duckdb");
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
                BuildImportSql(utf8CsvPath, shardPrefixLength),
                cancellationToken);
            await ExecuteNonQueryAsync(
                connection,
                BuildInsertAllSql(moduleUpdatedAt, shardPrefixLength),
                cancellationToken);
            await ExecuteNonQueryAsync(
                connection,
                $"COPY rntrc_output TO '{EscapeSqlLiteral(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)",
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

    private static string BuildImportSql(string csvPath, int shardPrefixLength)
    {
        return $@"
            CREATE TABLE transportadores AS
            SELECT
                nome_transportador,
                numero_rntrc,
                data_primeiro_cadastro,
                situacao_rntrc,
                CleanCnpj(cpfcnpjtransportador) AS cnpj,
                categoria_transportador,
                cep,
                municipio,
                uf,
                equiparado,
                data_situacao_rntrc
            FROM {BuildCsvRead(csvPath)}
            WHERE length(CleanCnpj(cpfcnpjtransportador)) = 14;

            CREATE TABLE rntrc_output (
                cnpj VARCHAR,
                cnpj_prefix VARCHAR,
                payload_json VARCHAR,
                content_hash VARCHAR,
                source_updated_at VARCHAR,
                module_updated_at VARCHAR
            );";
    }

    private static string BuildInsertAllSql(DateTimeOffset moduleUpdatedAt, int shardPrefixLength)
    {
        var updatedAt = EscapeSqlLiteral(moduleUpdatedAt.ToString("O"));

        return $@"
            INSERT INTO rntrc_output
            WITH ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY cnpj
                        ORDER BY numero_rntrc, nome_transportador
                    ) AS row_number
                FROM transportadores
            ),
            payloads AS (
                SELECT
                    cnpj,
                    substring(cnpj, 1, {shardPrefixLength}) AS cnpj_prefix,
                    to_json(struct_pack(
                        updated_at := '{updatedAt}',
                        numero_rntrc := COALESCE(numero_rntrc, ''),
                        nome := COALESCE(nome_transportador, ''),
                        categoria := COALESCE(categoria_transportador, ''),
                        situacao := COALESCE(situacao_rntrc, ''),
                        data_primeiro_cadastro := COALESCE(data_primeiro_cadastro, ''),
                        data_situacao := COALESCE(data_situacao_rntrc, ''),
                        cep := COALESCE(cep, ''),
                        municipio := COALESCE(municipio, ''),
                        uf := COALESCE(uf, ''),
                        equiparado := lower(COALESCE(equiparado, '')) IN ('sim', 's', 'true', '1')
                    )) AS payload_json
                FROM ranked
                WHERE row_number = 1
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

    private static string BuildCsvRead(string path)
    {
        return $@"
            read_csv(
                '{EscapeSqlLiteral(path)}',
                columns = {{
                    'nome_transportador': 'VARCHAR',
                    'numero_rntrc': 'VARCHAR',
                    'data_primeiro_cadastro': 'VARCHAR',
                    'situacao_rntrc': 'VARCHAR',
                    'cpfcnpjtransportador': 'VARCHAR',
                    'categoria_transportador': 'VARCHAR',
                    'cep': 'VARCHAR',
                    'municipio': 'VARCHAR',
                    'uf': 'VARCHAR',
                    'equiparado': 'VARCHAR',
                    'data_situacao_rntrc': 'VARCHAR'
                }},
                header = true,
                auto_detect = false,
                all_varchar = true,
                delim = ';',
                quote = '""',
                escape = '""',
                nullstr = '',
                ignore_errors = true
            )";
    }

    private static async Task<string> ConvertLatin1FileToUtf8Async(
        string sourcePath,
        string destinationPath,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(destinationPath)!);

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
