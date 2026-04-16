using System.Buffers.Binary;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Globalization;
using System.Text;
using CNPJExporter.Configuration;
using CNPJExporter.Processors;
using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ETL.Tests;

[TestClass]
public sealed class ShardGenerationRegressionTests
{
    private static readonly object CurrentDirectoryLock = new();
    private static readonly SemaphoreSlim ParquetRefreshLock = new(1, 1);
    private const string ItauCnpj = "60701190000104";
    private const string ItauPrefix = "607";
    private const string SampleShardCnpj = "60700007000148";
    private const int BinaryIndexHeaderSize = 8;
    private const int BinaryIndexEntrySize = 26;
    private const int BinaryIndexCnpjLength = 14;

    [TestMethod]
    public async Task CsvRead_ShouldContain_60701190000104_InEstabelecimentoSource()
    {
        using var scope = TestEnvironmentScope.Create();
        RequireCsvFixture();
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var csvPath = Path.Combine(Environment.CurrentDirectory, "extracted_data", "2026-03", "K3241.K03200Y0.D60314.ESTABELE");

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT COUNT(*)
            FROM read_csv(
                ['{EscapeSqlLiteral(csvPath)}'],
                auto_detect=false,
                sep=';',
                header=false,
                encoding='CP1252',
                ignore_errors=true,
                parallel=false,
                max_line_size=10000000,
                columns={{
                    'cnpj_basico': 'VARCHAR',
                    'cnpj_ordem': 'VARCHAR',
                    'cnpj_dv': 'VARCHAR',
                    'identificador_matriz_filial': 'VARCHAR',
                    'nome_fantasia': 'VARCHAR',
                    'situacao_cadastral': 'VARCHAR',
                    'data_situacao_cadastral': 'VARCHAR',
                    'motivo_situacao_cadastral': 'VARCHAR',
                    'nome_cidade_exterior': 'VARCHAR',
                    'codigo_pais': 'VARCHAR',
                    'data_inicio_atividade': 'VARCHAR',
                    'cnae_principal': 'VARCHAR',
                    'cnaes_secundarios': 'VARCHAR',
                    'tipo_logradouro': 'VARCHAR',
                    'logradouro': 'VARCHAR',
                    'numero': 'VARCHAR',
                    'complemento': 'VARCHAR',
                    'bairro': 'VARCHAR',
                    'cep': 'VARCHAR',
                    'uf': 'VARCHAR',
                    'codigo_municipio': 'VARCHAR',
                    'ddd1': 'VARCHAR',
                    'telefone1': 'VARCHAR',
                    'ddd2': 'VARCHAR',
                    'telefone2': 'VARCHAR',
                    'ddd_fax': 'VARCHAR',
                    'fax': 'VARCHAR',
                    'correio_eletronico': 'VARCHAR',
                    'situacao_especial': 'VARCHAR',
                    'data_situacao_especial': 'VARCHAR'
                }}
            )
            WHERE cnpj_basico = '60701190'
              AND cnpj_ordem = '0001'
              AND cnpj_dv = '04'";

        var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        Assert.AreEqual(1, count, "A configuração atual de read_csv deveria ler o registro 60701190/0001-04 do arquivo ESTABELE.");
    }

    [TestMethod]
    public async Task PartitionedParquet_ShouldContain_60701190000104_InEstabelecimentoView()
    {
        using var scope = TestEnvironmentScope.Create();
        await EnsureFreshParquetDataAsync();
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await LoadViewAsync(connection, "estabelecimento", "estabelecimento/**/*.parquet", hivePartitioning: true);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            SELECT COUNT(*)
            FROM estabelecimento
            WHERE cnpj_prefix = '607'
              AND cnpj_basico = '60701190'
              AND cnpj_ordem = '0001'
              AND cnpj_dv = '04'";

        var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        Assert.AreEqual(1, count, "O registro 60701190/0001-04 existe no CSV bruto e deveria existir na view Parquet.");
    }

    [TestMethod]
    public async Task PhysicalPartitionFiles_ShouldContain_60701190000104_InEstabelecimentoPartition607()
    {
        using var scope = TestEnvironmentScope.Create();
        await EnsureFreshParquetDataAsync();
        await using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var fullPath = Path.Combine(
            Environment.CurrentDirectory,
            AppConfig.Current.Paths.ParquetDir,
            "2026-03",
            "estabelecimento",
            "cnpj_prefix=607",
            "*.parquet");

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT cnpj_ordem, cnpj_dv
            FROM read_parquet('{EscapeSqlLiteral(fullPath)}')
            WHERE cnpj_basico = '60701190'
            ORDER BY cnpj_ordem, cnpj_dv";

        var combinations = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            combinations.Add($"{reader.GetString(0)}/{reader.GetString(1)}");
        }

        Assert.IsTrue(
            combinations.Contains("0001/04"),
            $"O registro 60701190/0001-04 deveria existir nos arquivos Parquet físicos da partição 607. Combinações encontradas: {string.Join(", ", combinations.Take(20))}");
    }


    [TestMethod]
    public async Task SingleQuery_ShouldFind_60701190000104_InRealDataset()
    {
        using var scope = TestEnvironmentScope.Create();
        await EnsureFreshParquetDataAsync();
        using var ingestor = new ParquetIngestor("2026-03");

        var outputDir = Path.Combine(scope.TempRoot, "single-output");
        await ingestor.ExportSingleCnpjAsync(ItauCnpj, outputDir);

        var filePath = Path.Combine(outputDir, "2026-03", $"{ItauCnpj}.json");
        Assert.IsTrue(File.Exists(filePath), "O CNPJ 60701190000104 existe no CSV bruto da Receita e deveria ser encontrado localmente.");
    }

    [TestMethod]
    public async Task Shard607_ShouldContain_CompanyFields_For60700007000148()
    {
        using var scope = TestEnvironmentScope.Create();
        await EnsureFreshParquetDataAsync();
        using var ingestor = new ParquetIngestor("2026-03");

        var outputRoot = Path.Combine(scope.TempRoot, "shard-output");
        await ingestor.ExportSingleShardAsync(ItauPrefix, outputRoot);

        var shardDataPath = Path.Combine(outputRoot, "2026-03", AppConfig.Current.Shards.RemoteDir, $"{ItauPrefix}.ndjson");
        var shardIndexPath = Path.Combine(outputRoot, "2026-03", AppConfig.Current.Shards.RemoteDir, $"{ItauPrefix}.index.bin");
        Assert.IsTrue(File.Exists(shardDataPath), $"O shard {ItauPrefix}.ndjson deveria ser gerado.");
        Assert.IsTrue(File.Exists(shardIndexPath), $"O índice {ItauPrefix}.index.bin deveria ser gerado.");

        var company = await FindCompanyInShardNdjsonAsync(shardDataPath, SampleShardCnpj);
        Assert.IsNotNull(company, $"O CNPJ {SampleShardCnpj} deveria existir no shard {ItauPrefix}.");

        AssertHasNonEmptyString(company, "razao_social");
        AssertHasNonEmptyString(company, "natureza_juridica");
        AssertHasNonEmptyString(company, "porte_empresa");
        AssertHasParsableMonetaryString(company, "capital_social");

        var indexBytes = await File.ReadAllBytesAsync(shardIndexPath);
        Assert.IsTrue(indexBytes.Length >= 8, "O índice binário deveria conter cabeçalho.");
        CollectionAssert.AreEqual("OCI1"u8.ToArray(), indexBytes[..4], "O cabeçalho do índice binário deveria conter a magic OCI1.");

        var recordCount = BinaryPrimitives.ReadUInt32LittleEndian(indexBytes.AsSpan(4, sizeof(uint)));
        Assert.IsTrue(recordCount > 0, "O índice binário deveria ter ao menos um registro.");
        Assert.AreEqual(
            BinaryIndexHeaderSize + ((int)recordCount * BinaryIndexEntrySize),
            indexBytes.Length,
            "Tamanho inesperado para o índice binário.");

        var firstEntryCnpj = Encoding.ASCII.GetString(indexBytes, BinaryIndexHeaderSize, BinaryIndexCnpjLength);
        Assert.IsFalse(string.IsNullOrWhiteSpace(firstEntryCnpj), "O primeiro CNPJ indexado não deveria ser vazio.");
    }

    [TestMethod]
    public async Task Shard607_ShouldNotInlineIntegrationPayload()
    {
        using var scope = TestEnvironmentScope.Create();
        await EnsureFreshParquetDataAsync();

        using var ingestor = new ParquetIngestor("2026-03");

        var outputRoot = Path.Combine(scope.TempRoot, "base-shard-output");
        await ingestor.ExportSingleShardAsync(ItauPrefix, outputRoot);

        var shardDataPath = Path.Combine(outputRoot, "2026-03", AppConfig.Current.Shards.RemoteDir, $"{ItauPrefix}.ndjson");
        var company = await FindCompanyInShardNdjsonAsync(shardDataPath, SampleShardCnpj);

        Assert.IsNotNull(company, $"O CNPJ {SampleShardCnpj} deveria existir no shard {ItauPrefix}.");

        Assert.IsFalse(
            company.ContainsKey("cno"),
            "O shard base deve conter apenas dados da Receita; módulos são montados no Worker.");
    }

    private sealed class TestEnvironmentScope : IDisposable
    {
        private readonly string _originalCurrentDirectory;

        public string TempRoot { get; }

        private TestEnvironmentScope(string tempRoot, string originalCurrentDirectory)
        {
            TempRoot = tempRoot;
            _originalCurrentDirectory = originalCurrentDirectory;
        }

        public static TestEnvironmentScope Create()
        {
            var repoRoot = ResolveRepoRoot();
            var etlRoot = Path.Combine(repoRoot, "src", "ETL", "Processor");
            var originalCurrentDirectory = Environment.CurrentDirectory;
            lock (CurrentDirectoryLock)
            {
                Environment.CurrentDirectory = etlRoot;
                AppConfig.Load(Path.Combine(etlRoot, "config.json"));
            }

            var tempRoot = Path.Combine(Path.GetTempPath(), $"opencnpj-regression-{Guid.NewGuid():N}");
            Directory.CreateDirectory(tempRoot);

            return new TestEnvironmentScope(tempRoot, originalCurrentDirectory);
        }

        private static string ResolveRepoRoot()
        {
            var current = new DirectoryInfo(AppContext.BaseDirectory);
            while (current is not null)
            {
                var candidate = Path.Combine(current.FullName, "src", "ETL", "Processor");
                if (Directory.Exists(candidate))
                    return current.FullName;

                current = current.Parent;
            }

            throw new DirectoryNotFoundException("Não foi possível localizar a raiz do repositório para os testes de regressão.");
        }

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(TempRoot))
                    Directory.Delete(TempRoot, true);
            }
            finally
            {
                lock (CurrentDirectoryLock)
                {
                    Environment.CurrentDirectory = _originalCurrentDirectory;
                }
            }
        }
    }

    private static async Task LoadViewAsync(DuckDBConnection connection, string name, string pattern, bool hivePartitioning)
    {
        var baseDir = Path.Combine(Environment.CurrentDirectory, AppConfig.Current.Paths.ParquetDir, "2026-03");
        var fullPath = Path.Combine(baseDir, pattern);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = hivePartitioning
            ? $"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{fullPath}', hive_partitioning = true, hive_types = {{'cnpj_prefix': VARCHAR}})"
            : $"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{fullPath}')";
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task EnsureFreshParquetDataAsync()
    {
        await ParquetRefreshLock.WaitAsync();
        try
        {
            if (HasParquetFixture())
                return;

            RequireCsvFixture();
            using var ingestor = new ParquetIngestor("2026-03");
            await ingestor.ConvertCsvsToParquet();
        }
        finally
        {
            ParquetRefreshLock.Release();
        }
    }

    private static void RequireCsvFixture()
    {
        var csvPath = Path.Combine(
            Environment.CurrentDirectory,
            "extracted_data",
            "2026-03",
            "K3241.K03200Y0.D60314.ESTABELE");

        if (!File.Exists(csvPath))
            Assert.Inconclusive($"Fixture local ausente: {csvPath}");
    }

    private static bool HasParquetFixture()
    {
        var estabelecimentoDir = Path.Combine(
            Environment.CurrentDirectory,
            AppConfig.Current.Paths.ParquetDir,
            "2026-03",
            "estabelecimento");

        return Directory.Exists(estabelecimentoDir)
               && Directory.EnumerateFiles(estabelecimentoDir, "*.parquet", SearchOption.AllDirectories).Any();
    }

    private static string EscapeSqlLiteral(string value)
    {
        return value.Replace("'", "''");
    }

    private static void AssertHasNonEmptyString(JsonObject company, string propertyName)
    {
        Assert.IsTrue(
            company.TryGetPropertyValue(propertyName, out var node) &&
            node is JsonValue &&
            !string.IsNullOrWhiteSpace(node.GetValue<string>()),
            $"O campo '{propertyName}' deveria existir e conter texto.");
    }

    private static void AssertHasParsableMonetaryString(JsonObject company, string propertyName)
    {
        Assert.IsTrue(
            company.TryGetPropertyValue(propertyName, out var node) &&
            node is JsonValue,
            $"O campo '{propertyName}' deveria existir.");

        var value = node!.GetValue<string>();
        Assert.IsFalse(string.IsNullOrWhiteSpace(value), $"O campo '{propertyName}' deveria conter um valor.");
        Assert.IsTrue(
            decimal.TryParse(value, NumberStyles.Number, new CultureInfo("pt-BR"), out _),
            $"O campo '{propertyName}' deveria conter um valor monetário parseável em pt-BR. Valor atual: '{value}'.");
    }

    private static async Task<JsonObject?> FindCompanyInShardNdjsonAsync(string shardPath, string cnpj)
    {
        await using var stream = File.OpenRead(shardPath);
        using var reader = new StreamReader(stream);

        while (await reader.ReadLineAsync() is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var json = JsonNode.Parse(line)?.AsObject();
            if (json?["cnpj"]?.GetValue<string>() != cnpj)
                continue;

            return json;
        }

        return null;
    }
}
