using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GtfsContainer.Services;

public interface ISqlLoggingService
{
    Task LogAsync(LogLevel logLevel, string? stepName, string? runId, string message, string? exceptionDetails = null);
}

public class SqlLoggingService : ISqlLoggingService
{
    private readonly string? _connectionString;
    private readonly ILogger<SqlLoggingService> _logger;

    public SqlLoggingService(IConfiguration configuration, ILogger<SqlLoggingService> logger)
    {
        _connectionString = configuration["SqlConnectionString"];
        _logger = logger;

        if (string.IsNullOrEmpty(_connectionString))
            _logger.LogWarning("SqlConnectionString non configurato: SQL logging disabilitato.");
    }

    public async Task LogAsync(LogLevel logLevel, string? stepName, string? runId, string message, string? exceptionDetails = null)
    {
        // Scrivi sempre su console/Application Insights
        _logger.Log(logLevel, "[{Step}] {Message}", stepName, message);

        if (string.IsNullOrEmpty(_connectionString))
            return;

        const string sql = @"
            INSERT INTO dbo.GtfsImportLogs
                (Timestamp, LogLevel, FunctionName, OrchestrationInstanceId, Message, ExceptionDetails)
            VALUES
                (GETUTCDATE(), @LogLevel, @FunctionName, @OrchestrationInstanceId, @Message, @ExceptionDetails)";

        try
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@LogLevel", logLevel.ToString());
            cmd.Parameters.AddWithValue("@FunctionName", (object?)stepName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@OrchestrationInstanceId", (object?)runId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Message", (object?)message ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@ExceptionDetails", (object?)exceptionDetails ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Impossibile scrivere log su SQL: {Original}", message);
        }
    }
}
