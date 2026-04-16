namespace CNPJExporter.Modules.Receita.Processors;

internal static class Sql
{
    public static string EscapeLiteral(string value)
    {
        return value.Replace("'", "''");
    }
}
