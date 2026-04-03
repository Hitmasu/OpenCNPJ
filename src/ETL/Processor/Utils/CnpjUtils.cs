using System.Text.RegularExpressions;

namespace CNPJExporter.Utils;

/// <summary>
/// Utilitários para validação e normalização de CNPJ (incluindo alfanumérico)
/// </summary>
public static class CnpjUtils
{
    private static readonly Regex MaskCharacters = new(@"[./-]", RegexOptions.Compiled);
    private static readonly Regex InvalidCharacters = new(@"[^A-Z\d./-]", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly Regex BaseCnpjPattern = new(@"^[A-Z\d]{12}$", RegexOptions.Compiled);
    private static readonly Regex FullCnpjPattern = new(@"^[A-Z\d]{12}\d{2}$", RegexOptions.Compiled);
    private const int BaseLength = 12;

    /// <summary>
    /// Remove máscara do CNPJ (pontos, barras, hífens) e converte para maiúsculas
    /// </summary>
    public static string RemoveMask(string? cnpj)
    {
        if (string.IsNullOrWhiteSpace(cnpj))
            return string.Empty;
            
        return MaskCharacters.Replace(cnpj, "").ToUpperInvariant();
    }

    /// <summary>
    /// Valida se o CNPJ tem formato válido (alfanumérico: 12 caracteres alfanuméricos + 2 dígitos)
    /// </summary>
    public static bool IsValidFormat(string? cnpj)
    {
        if (string.IsNullOrWhiteSpace(cnpj))
            return false;

        if (InvalidCharacters.IsMatch(cnpj))
            return false;

        var raw = RemoveMask(cnpj);

        if (raw.Length != 14)
            return false;

        if (!FullCnpjPattern.IsMatch(raw))
            return false;

        if (IsRepeatedSequence(raw))
            return false;

        return true;
    }

    /// <summary>
    /// Verifica se é uma sequência repetida (ex: 11111111111111 ou AAAAAAAAAAAAAA)
    /// </summary>
    private static bool IsRepeatedSequence(string cnpj)
    {
        if (string.IsNullOrEmpty(cnpj) || cnpj.Length < 2)
            return false;

        var firstChar = cnpj[0];
        return cnpj.All(c => c == firstChar);
    }

    /// <summary>
    /// Extrai as partes do CNPJ: básico (8), ordem (4), dígito verificador (2)
    /// Remove máscara automaticamente antes de extrair
    /// </summary>
    public static (string basico, string ordem, string dv) ParseCnpj(string cnpj)
    {
        var raw = RemoveMask(cnpj);
        
        if (raw.Length != 14)
            throw new ArgumentException($"CNPJ deve ter 14 caracteres após remover máscara. Recebido: {raw.Length}", nameof(cnpj));

        var basico = raw[..8];
        var ordem = raw.Substring(8, 4);
        var dv = raw.Substring(12, 2);

        return (basico, ordem, dv);
    }
}
