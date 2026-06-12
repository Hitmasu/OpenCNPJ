using System.ComponentModel;
using System.Diagnostics;

namespace CNPJExporter.Processors.BigQuery;

internal sealed class ProcessBigQueryCommandRunner : IBigQueryCommandRunner
{
    public async Task RunAsync(
        string executable,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken = default)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = executable,
            UseShellExecute = false,
            RedirectStandardError = true,
            RedirectStandardOutput = true
        };

        foreach (var argument in arguments)
            startInfo.ArgumentList.Add(argument);

        using var process = new Process { StartInfo = startInfo };
        try
        {
            process.Start();
        }
        catch (Win32Exception ex)
        {
            throw new InvalidOperationException($"Comando BigQuery não encontrado: {executable}.", ex);
        }

        var outputTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var errorTask = process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);

        var output = await outputTask;
        var error = await errorTask;
        if (process.ExitCode == 0)
            return;

        var details = string.IsNullOrWhiteSpace(error) ? output : error;
        throw new InvalidOperationException(
            $"Comando BigQuery falhou com exit code {process.ExitCode}: {executable} {Describe(arguments)}. {Truncate(details)}");
    }

    private static string Describe(IEnumerable<string> arguments) =>
        string.Join(" ", arguments.Select(argument => argument.Contains(' ', StringComparison.Ordinal) ? $"\"{argument}\"" : argument));

    private static string Truncate(string value)
    {
        var normalized = value.Trim();
        return normalized.Length <= 2_000 ? normalized : normalized[..2_000] + "...";
    }
}
