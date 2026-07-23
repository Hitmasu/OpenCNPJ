using System.Text.Json;
using CNPJExporter.Modules.Receita;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ReceitaBigQueryParquetExporterTests
{
    [TestMethod]
    public void GetSource_ShouldExposeSingleCanonicalReceitaSource()
    {
        var parquetRoot = Path.Combine(Path.GetTempPath(), "opencnpj", "parquet", "2026-04");

        var source = BigQueryParquetExporter.GetSource(parquetRoot);

        Assert.AreEqual("receita", source.TableName);
        Assert.AreEqual(0, source.SourcePaths.Count);
    }

    [TestMethod]
    public async Task MaterializeAsync_ShouldCreateColumnarParquetPartsForReceita()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-receita-bigquery-{Guid.NewGuid():N}");
        var parquetRoot = Path.Combine(tempRoot, "parquet");

        try
        {
            await using (var connection = new DuckDBConnection("Data Source=:memory:"))
            {
                await connection.OpenAsync();
                await SeedReceitaProjectionDataAsync(connection, parquetRoot);
            }

            var outputPaths = await new BigQueryParquetExporter(parquetRoot, shardPrefixLength: 3)
                .MaterializeAsync("2026-04");

            Assert.AreEqual(1, outputPaths.Count);
            Assert.IsTrue(File.Exists(outputPaths.Single()));

            var source = BigQueryParquetExporter.GetSource(parquetRoot);
            CollectionAssert.AreEqual(outputPaths.ToArray(), source.SourcePaths.ToArray());

            await using (var connection = new DuckDBConnection("Data Source=:memory:"))
            {
                await connection.OpenAsync();
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $@"
                    SELECT
                        cnpj,
                        cnpj_prefix,
                        razao_social,
                        nome_fantasia,
                        situacao_cadastral,
                        data_situacao_cadastral,
                        matriz_filial,
                        cnae_principal,
                        capital_social,
                        opcao_simples,
                        data_exclusao_simples,
                        data_exclusao_mei,
                        codigo_municipio,
                        ente_federativo,
                        nome_cidade_exterior,
                        codigo_pais,
                        to_json(cnaes) AS cnaes_json,
                        to_json(qualificacao_responsavel) AS qualificacao_responsavel_json,
                        to_json(motivo_situacao_cadastral) AS motivo_situacao_cadastral_json,
                        to_json(pais) AS pais_json,
                        to_json(QSA) AS qsa_json
                    FROM read_parquet('{EscapeSqlLiteral(outputPaths.Single())}')";

                await using var reader = await cmd.ExecuteReaderAsync();
                Assert.IsTrue(await reader.ReadAsync());

                Assert.AreEqual("12ABC34501DE35", reader.GetString(0));
                Assert.AreEqual("12A", reader.GetString(1));
                Assert.AreEqual("EMPRESA TESTE LTDA", reader.GetString(2));
                Assert.AreEqual("EMPRESA TESTE", reader.GetString(3));
                Assert.AreEqual("Ativa", reader.GetString(4));
                Assert.AreEqual(new DateTime(2024, 1, 15), reader.GetDateTime(5));
                Assert.AreEqual("Matriz", reader.GetString(6));
                Assert.AreEqual("6201501", reader.GetString(7));
                Assert.AreEqual(10000.00m, reader.GetDecimal(8));
                Assert.IsTrue(reader.GetBoolean(9));
                Assert.AreEqual(new DateTime(2024, 2, 20), reader.GetDateTime(10));
                Assert.AreEqual(new DateTime(2024, 3, 21), reader.GetDateTime(11));
                Assert.AreEqual("7107", reader.GetString(12));
                Assert.AreEqual("ENTE TESTE", reader.GetString(13));
                Assert.AreEqual("MONTEVIDEO", reader.GetString(14));
                Assert.AreEqual("105", reader.GetString(15));

                using var cnaes = JsonDocument.Parse(reader.GetString(16));
                Assert.AreEqual(2, cnaes.RootElement.GetArrayLength());
                Assert.AreEqual("6201501", cnaes.RootElement[0].GetProperty("codigo").GetString());
                Assert.IsTrue(cnaes.RootElement[0].GetProperty("is_principal").GetBoolean());
                Assert.AreEqual("6202300", cnaes.RootElement[1].GetProperty("codigo").GetString());

                using var qualificacao = JsonDocument.Parse(reader.GetString(17));
                Assert.AreEqual("49", qualificacao.RootElement.GetProperty("codigo").GetString());
                Assert.AreEqual("Sócio-Administrador", qualificacao.RootElement.GetProperty("descricao").GetString());

                using var motivo = JsonDocument.Parse(reader.GetString(18));
                Assert.AreEqual("01", motivo.RootElement.GetProperty("codigo").GetString());
                Assert.AreEqual("Extinção por Encerramento Liquidação Voluntária", motivo.RootElement.GetProperty("descricao").GetString());

                using var pais = JsonDocument.Parse(reader.GetString(19));
                Assert.AreEqual("105", pais.RootElement.GetProperty("codigo").GetString());
                Assert.AreEqual("BRASIL", pais.RootElement.GetProperty("descricao").GetString());

                using var qsa = JsonDocument.Parse(reader.GetString(20));
                Assert.AreEqual(1, qsa.RootElement.GetArrayLength());
                Assert.AreEqual("249", qsa.RootElement[0].GetProperty("codigo_pais").GetString());
                Assert.AreEqual("ESTADOS UNIDOS", qsa.RootElement[0].GetProperty("pais").GetProperty("descricao").GetString());
                Assert.AreEqual("12345678901", qsa.RootElement[0].GetProperty("representante_legal").GetString());
                Assert.AreEqual("05", qsa.RootElement[0].GetProperty("qualificacao_representante").GetProperty("codigo").GetString());

                Assert.IsFalse(await reader.ReadAsync());
            }
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    private static async Task SeedReceitaProjectionDataAsync(DuckDBConnection connection, string parquetDir)
    {
        await ExecuteAsync(connection, """
            CREATE TABLE natureza(codigo VARCHAR, descricao VARCHAR);
            INSERT INTO natureza VALUES ('2062', 'Sociedade Empresária Limitada');
            CREATE TABLE municipio(codigo VARCHAR, descricao VARCHAR);
            INSERT INTO municipio VALUES ('7107', 'SAO PAULO');
            CREATE TABLE motivo(codigo VARCHAR, descricao VARCHAR);
            INSERT INTO motivo VALUES ('01', 'Extinção por Encerramento Liquidação Voluntária');
            CREATE TABLE pais(codigo VARCHAR, descricao VARCHAR);
            INSERT INTO pais VALUES ('105', 'BRASIL'), ('249', 'ESTADOS UNIDOS');
            CREATE TABLE qualificacao(codigo VARCHAR, descricao VARCHAR);
            INSERT INTO qualificacao VALUES ('49', 'Sócio-Administrador'), ('05', 'Administrador'), ('22', 'Sócio');
            CREATE TABLE cnae(codigo VARCHAR, descricao VARCHAR);
            INSERT INTO cnae VALUES
                ('6201501', 'Desenvolvimento de programas de computador sob encomenda'),
                ('6202300', 'Desenvolvimento e licenciamento de programas de computador customizáveis');
            """);

        foreach (var tableName in new[] { "natureza", "municipio", "motivo", "pais", "qualificacao", "cnae" })
            await CopyFlatAsync(connection, parquetDir, tableName);

        await CopyPartitionedAsync(connection, parquetDir, "estabelecimento", """
            SELECT
                '12A' AS cnpj_prefix,
                '12ABC345' AS cnpj_basico,
                '01DE' AS cnpj_ordem,
                '35' AS cnpj_dv,
                '1' AS identificador_matriz_filial,
                'EMPRESA TESTE' AS nome_fantasia,
                '02' AS situacao_cadastral,
                '20240115' AS data_situacao_cadastral,
                '01' AS motivo_situacao_cadastral,
                'MONTEVIDEO' AS nome_cidade_exterior,
                '105' AS codigo_pais,
                '20240115' AS data_inicio_atividade,
                '6201501' AS cnae_principal,
                '6202300' AS cnaes_secundarios,
                'RUA' AS tipo_logradouro,
                'EXEMPLO' AS logradouro,
                '100' AS numero,
                'SALA 01' AS complemento,
                'CENTRO' AS bairro,
                '00000000' AS cep,
                'SP' AS uf,
                '7107' AS codigo_municipio,
                '11' AS ddd1,
                '999999999' AS telefone1,
                CAST(NULL AS VARCHAR) AS ddd2,
                CAST(NULL AS VARCHAR) AS telefone2,
                CAST(NULL AS VARCHAR) AS ddd_fax,
                CAST(NULL AS VARCHAR) AS fax,
                'contato@example.invalid' AS correio_eletronico,
                'BAIXA ESPECIAL' AS situacao_especial,
                '20250131' AS data_situacao_especial
            """);

        await CopyPartitionedAsync(connection, parquetDir, "empresa", """
            SELECT
                '12A' AS cnpj_prefix,
                '12ABC345' AS cnpj_basico,
                'EMPRESA TESTE LTDA' AS razao_social,
                '2062' AS natureza_juridica,
                '49' AS qualificacao_responsavel,
                '10000,00' AS capital_social,
                '01' AS porte_empresa,
                'ENTE TESTE' AS ente_federativo
            """);

        await CopyPartitionedAsync(connection, parquetDir, "simples", """
            SELECT
                '12A' AS cnpj_prefix,
                '12ABC345' AS cnpj_basico,
                'S' AS opcao_simples,
                '20240115' AS data_opcao_simples,
                '20240220' AS data_exclusao_simples,
                'N' AS opcao_mei,
                '' AS data_opcao_mei,
                '20240321' AS data_exclusao_mei
            """);

        await CopyPartitionedAsync(connection, parquetDir, "socio", """
            SELECT
                '12A' AS cnpj_prefix,
                '12ABC345' AS cnpj_basico,
                '2' AS identificador_socio,
                'SOCIO TESTE' AS nome_socio,
                '***000000**' AS cnpj_cpf_socio,
                '22' AS qualificacao_socio,
                '20240115' AS data_entrada_sociedade,
                '249' AS codigo_pais,
                '12345678901' AS representante_legal,
                'REPRESENTANTE TESTE' AS nome_representante,
                '05' AS qualificacao_representante,
                '5' AS faixa_etaria
            """);
    }

    private static async Task CopyFlatAsync(
        DuckDBConnection connection,
        string parquetDir,
        string tableName)
    {
        Directory.CreateDirectory(parquetDir);
        await ExecuteAsync(connection, $"""
            COPY {tableName}
            TO '{EscapeSqlLiteral(Path.Combine(parquetDir, $"{tableName}.parquet"))}'
            (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)
            """);
    }

    private static async Task CopyPartitionedAsync(
        DuckDBConnection connection,
        string parquetDir,
        string tableName,
        string selectSql)
    {
        var tableDir = Path.Combine(parquetDir, tableName);
        Directory.CreateDirectory(tableDir);
        await ExecuteAsync(connection, $"""
            COPY ({selectSql})
            TO '{EscapeSqlLiteral(tableDir)}'
            (
                FORMAT PARQUET,
                PARTITION_BY (cnpj_prefix)
            )
            """);
    }

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
