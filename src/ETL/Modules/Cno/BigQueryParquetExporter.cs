using CNPJExporter.Integrations;
using DuckDB.NET.Data;
using Spectre.Console;

namespace CNPJExporter.Modules.Cno;

public sealed class BigQueryParquetExporter
{
    public const string TableName = "cno";

    private const string ObrasJsonSchema = """[{"cno":"VARCHAR","nome":"VARCHAR","nome_empresarial":"VARCHAR","situacao":{"codigo":"VARCHAR","descricao":"VARCHAR"},"data_inicio":"VARCHAR","data_inicio_responsabilidade":"VARCHAR","data_registro":"VARCHAR","data_situacao":"VARCHAR","cep":"VARCHAR","uf":"VARCHAR","codigo_municipio":"VARCHAR","municipio":"VARCHAR","tipo_logradouro":"VARCHAR","logradouro":"VARCHAR","numero":"VARCHAR","bairro":"VARCHAR","complemento":"VARCHAR","unidade_medida":"VARCHAR","area_total":"VARCHAR","cno_vinculado":"VARCHAR","codigo_pais":"VARCHAR","pais":"VARCHAR","qualificacao_responsavel":{"codigo":"VARCHAR","descricao":"VARCHAR"},"codigo_localizacao":"VARCHAR"}]""";

    private readonly string _sourceParquetPath;

    public BigQueryParquetExporter(string sourceParquetPath)
    {
        _sourceParquetPath = sourceParquetPath;
    }

    public async Task<BigQueryParquetSource> MaterializeAsync(CancellationToken cancellationToken = default)
    {
        if (!File.Exists(_sourceParquetPath))
            throw new FileNotFoundException("Parquet canônico do CNO não encontrado.", _sourceParquetPath);

        var outputPath = GetOutputPath(_sourceParquetPath);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
        DeleteIfExists(outputPath);

        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await ConfigureAsync(connection, outputPath, cancellationToken);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            COPY (
                SELECT
                    cnpj,
                    UPPER(cnpj_prefix) AS cnpj_prefix,
                    json_extract_string(payload_json, '$.updated_at') AS updated_at,
                    from_json(json_extract(payload_json, '$.obras'), '{EscapeSqlLiteral(ObrasJsonSchema)}') AS obras
                FROM read_parquet('{EscapeSqlLiteral(_sourceParquetPath)}')
            )
            TO '{EscapeSqlLiteral(outputPath)}'
            (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)";
        await cmd.ExecuteNonQueryAsync(cancellationToken);

        AnsiConsole.MarkupLine($"[grey]BigQuery CNO:[/] Parquet columnar gerado em [grey]{outputPath.EscapeMarkup()}[/]");
        return GetSource(_sourceParquetPath);
    }

    public static BigQueryParquetSource GetSource(string sourceParquetPath)
    {
        var outputPath = GetOutputPath(sourceParquetPath);
        IReadOnlyList<string> sourcePaths = File.Exists(outputPath)
            ? [outputPath]
            : Array.Empty<string>();

        return new BigQueryParquetSource(TableName, sourcePaths);
    }

    private static string GetOutputPath(string sourceParquetPath) =>
        Path.Combine(Path.GetDirectoryName(sourceParquetPath)!, "bigquery", $"{TableName}.parquet");

    private static async Task ConfigureAsync(
        DuckDBConnection connection,
        string outputPath,
        CancellationToken cancellationToken)
    {
        var tempDir = Path.Combine(Path.GetDirectoryName(outputPath)!, "_duckdb_temp");
        Directory.CreateDirectory(tempDir);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SET threads = 4;
            SET memory_limit = '8GB';
            SET preserve_insertion_order = false;
            SET temp_directory = '{EscapeSqlLiteral(tempDir)}';
            SET max_temp_directory_size = '200GB';";
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
