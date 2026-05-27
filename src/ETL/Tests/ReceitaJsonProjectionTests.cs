using System.Text.Json;
using CNPJExporter.Modules.Receita.Processors;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ReceitaJsonProjectionTests
{
    [TestMethod]
    public void BuildJsonQueryForPrefixBatch_ShouldNotSortEnrichedJsonPayloadInDuckDb()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-receita-query-{Guid.NewGuid():N}");
        var parquetDir = Path.Combine(tempRoot, "parquet");

        try
        {
            CreatePartitionMarker(parquetDir, "estabelecimento", "607");
            CreatePartitionMarker(parquetDir, "empresa", "607");

            var query = new ShardQueryBuilder(parquetDir)
                .BuildJsonQueryForPrefixBatch(["607"], includeCnpjColumn: true, jsonAlias: "json_data");

            Assert.IsFalse(
                query.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase),
                "O batch de Receita deve streamar o JSON enriquecido sem sort global no DuckDB; o índice binário ordena apenas CNPJ/offset/length.");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public void BuildJsonQueryForCnpj_ShouldFilterQsaCteByCnpjBasico()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-receita-query-{Guid.NewGuid():N}");
        var parquetDir = Path.Combine(tempRoot, "parquet");

        try
        {
            CreatePartitionMarker(parquetDir, "estabelecimento", "607");
            CreatePartitionMarker(parquetDir, "empresa", "607");
            CreatePartitionMarker(parquetDir, "socio", "607");

            var query = new ShardQueryBuilder(parquetDir)
                .BuildJsonQueryForCnpj("607", "60701190", "0001", "04", "json_output");

            StringAssert.Contains(query, "s.cnpj_basico = '60701190'");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public void BuildJsonQueryForPrefixBatch_ShouldFilterShardInputsByCnpjBasicoRange()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-receita-query-{Guid.NewGuid():N}");
        var parquetDir = Path.Combine(tempRoot, "parquet");

        try
        {
            CreatePartitionMarker(parquetDir, "estabelecimento", "607");
            CreatePartitionMarker(parquetDir, "empresa", "607");
            CreatePartitionMarker(parquetDir, "simples", "607");
            CreatePartitionMarker(parquetDir, "socio", "607");

            var query = new ShardQueryBuilder(parquetDir)
                .BuildJsonQueryForPrefixBatch(
                    ["607"],
                    includeCnpjColumn: true,
                    jsonAlias: "json_data",
                    cnpjBasicoStartInclusive: "60700000",
                    cnpjBasicoEndExclusive: "60710000");

            StringAssert.Contains(query, "cnpj_basico >= '60700000'");
            StringAssert.Contains(query, "cnpj_basico < '60710000'");
            StringAssert.Contains(query, "s.cnpj_basico >= '60700000'");
            StringAssert.Contains(query, "s.cnpj_basico < '60710000'");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public void BuildJsonQueryForPrefixBatch_ShouldUseMaterializedQsaWhenAvailable()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-receita-query-{Guid.NewGuid():N}");
        var parquetDir = Path.Combine(tempRoot, "parquet");

        try
        {
            CreatePartitionMarker(parquetDir, "estabelecimento", "607");
            CreatePartitionMarker(parquetDir, "empresa", "607");
            CreatePartitionMarker(parquetDir, "simples", "607");
            CreatePartitionMarker(parquetDir, "socio", "607");
            CreatePartitionMarker(parquetDir, "qsa", "607");

            var query = new ShardQueryBuilder(parquetDir)
                .BuildJsonQueryForPrefixBatch(
                    ["607"],
                    includeCnpjColumn: true,
                    jsonAlias: "json_data",
                    cnpjBasicoStartInclusive: "60700000",
                    cnpjBasicoEndExclusive: "60710000");

            StringAssert.Contains(query, "/qsa/cnpj_prefix=607/");
            StringAssert.Contains(query, "SELECT cnpj_prefix, cnpj_basico, qsa_data");
            Assert.IsFalse(
                query.Contains("/socio/cnpj_prefix=607/", StringComparison.OrdinalIgnoreCase),
                "Com QSA materializado, a geração de shard não deve ler a partição bruta de sócios.");
        }
        finally
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public async Task BuildJsonQueryForCnpj_ShouldExposeEnrichedReceitaFields()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-receita-json-{Guid.NewGuid():N}");
        var parquetDir = Path.Combine(tempRoot, "parquet");
        Directory.CreateDirectory(parquetDir);

        try
        {
            await using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync();
            await SeedReceitaProjectionDataAsync(connection, parquetDir);

            var query = new ShardQueryBuilder(parquetDir)
                .BuildJsonQueryForCnpj("607", "60701190", "0001", "04", "json_output");

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = query;
            var scalar = await cmd.ExecuteScalarAsync();
            Assert.IsNotNull(scalar);
            var payload = scalar.ToString();
            Assert.IsFalse(string.IsNullOrWhiteSpace(payload));
            using var document = JsonDocument.Parse(payload);
            var root = document.RootElement;

            Assert.AreEqual("49", root.GetProperty("qualificacao_responsavel").GetProperty("codigo").GetString());
            Assert.AreEqual("Sócio-Administrador", root.GetProperty("qualificacao_responsavel").GetProperty("descricao").GetString());
            Assert.AreEqual("ENTE TESTE", root.GetProperty("ente_federativo").GetString());
            Assert.AreEqual("01", root.GetProperty("motivo_situacao_cadastral").GetProperty("codigo").GetString());
            Assert.AreEqual("Extinção por Encerramento Liquidação Voluntária", root.GetProperty("motivo_situacao_cadastral").GetProperty("descricao").GetString());
            Assert.AreEqual("MONTEVIDEO", root.GetProperty("nome_cidade_exterior").GetString());
            Assert.AreEqual("105", root.GetProperty("codigo_pais").GetString());
            Assert.AreEqual("105", root.GetProperty("pais").GetProperty("codigo").GetString());
            Assert.AreEqual("BRASIL", root.GetProperty("pais").GetProperty("descricao").GetString());
            Assert.AreEqual("7107", root.GetProperty("codigo_municipio").GetString());
            Assert.AreEqual("BAIXA ESPECIAL", root.GetProperty("situacao_especial").GetString());
            Assert.AreEqual("2025-01-31", root.GetProperty("data_situacao_especial").GetString());
            Assert.AreEqual("2024-02-20", root.GetProperty("data_exclusao_simples").GetString());
            Assert.AreEqual("2024-03-21", root.GetProperty("data_exclusao_mei").GetString());

            var cnaes = root.GetProperty("cnaes");
            Assert.AreEqual(3, cnaes.GetArrayLength());
            Assert.AreEqual("6201501", cnaes[0].GetProperty("codigo").GetString());
            Assert.AreEqual("Desenvolvimento de programas de computador sob encomenda", cnaes[0].GetProperty("descricao").GetString());
            Assert.IsTrue(cnaes[0].GetProperty("is_principal").GetBoolean());
            Assert.AreEqual("6202300", cnaes[1].GetProperty("codigo").GetString());
            Assert.IsFalse(cnaes[1].GetProperty("is_principal").GetBoolean());

            var qsa = root.GetProperty("QSA")[0];
            Assert.AreEqual("249", qsa.GetProperty("codigo_pais").GetString());
            Assert.AreEqual("249", qsa.GetProperty("pais").GetProperty("codigo").GetString());
            Assert.AreEqual("ESTADOS UNIDOS", qsa.GetProperty("pais").GetProperty("descricao").GetString());
            Assert.AreEqual("12345678901", qsa.GetProperty("representante_legal").GetString());
            Assert.AreEqual("REPRESENTANTE TESTE", qsa.GetProperty("nome_representante").GetString());
            Assert.AreEqual("05", qsa.GetProperty("qualificacao_representante").GetProperty("codigo").GetString());
            Assert.AreEqual("Administrador", qsa.GetProperty("qualificacao_representante").GetProperty("descricao").GetString());
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
                ('6202300', 'Desenvolvimento e licenciamento de programas de computador customizáveis'),
                ('6311900', 'Tratamento de dados, provedores de serviços de aplicação e serviços de hospedagem na internet');
            """);

        await CopyPartitionedAsync(connection, parquetDir, "estabelecimento", """
            SELECT
                '607' AS cnpj_prefix,
                '60701190' AS cnpj_basico,
                '0001' AS cnpj_ordem,
                '04' AS cnpj_dv,
                '1' AS identificador_matriz_filial,
                'EMPRESA TESTE' AS nome_fantasia,
                '02' AS situacao_cadastral,
                '20240115' AS data_situacao_cadastral,
                '01' AS motivo_situacao_cadastral,
                'MONTEVIDEO' AS nome_cidade_exterior,
                '105' AS codigo_pais,
                '20240115' AS data_inicio_atividade,
                '6201501' AS cnae_principal,
                '6202300,6311900' AS cnaes_secundarios,
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
                '607' AS cnpj_prefix,
                '60701190' AS cnpj_basico,
                'EMPRESA TESTE LTDA' AS razao_social,
                '2062' AS natureza_juridica,
                '49' AS qualificacao_responsavel,
                '10000,00' AS capital_social,
                '01' AS porte_empresa,
                'ENTE TESTE' AS ente_federativo
            """);

        await CopyPartitionedAsync(connection, parquetDir, "simples", """
            SELECT
                '607' AS cnpj_prefix,
                '60701190' AS cnpj_basico,
                'S' AS opcao_simples,
                '20240115' AS data_opcao_simples,
                '20240220' AS data_exclusao_simples,
                'N' AS opcao_mei,
                '' AS data_opcao_mei,
                '20240321' AS data_exclusao_mei
            """);

        await CopyPartitionedAsync(connection, parquetDir, "socio", """
            SELECT
                '607' AS cnpj_prefix,
                '60701190' AS cnpj_basico,
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

    private static void CreatePartitionMarker(string parquetDir, string tableName, string prefix)
    {
        var partitionDir = Path.Combine(parquetDir, tableName, $"cnpj_prefix={prefix}");
        Directory.CreateDirectory(partitionDir);
        File.WriteAllBytes(Path.Combine(partitionDir, "part.parquet"), []);
    }
}
