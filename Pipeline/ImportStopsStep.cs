using Azure.Storage.Blobs;
using GtfsContainer.Services;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Text;

namespace GtfsContainer.Pipeline;

/// <summary>
/// Porta la logica di FiImportStopsActivity.
/// Legge stops.txt dal blob, filtra sugli stop_id usati in daily_stop_times_trips,
/// inserisce in daily_stops con il campo geography::Point.
/// </summary>
public class ImportStopsStep
{
    private readonly string _connectionString;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly ISqlLoggingService _sqlLogger;
    private readonly string _containerName;

    private const int BatchSize = 300; // max 2100 params SQL Server: 300 × 6 = 1800

    public ImportStopsStep(
        IConfiguration configuration,
        BlobServiceClient blobServiceClient,
        ISqlLoggingService sqlLogger)
    {
        _connectionString = configuration["SqlConnectionString"] ?? throw new InvalidOperationException("SqlConnectionString non configurato");
        _blobServiceClient = blobServiceClient;
        _sqlLogger = sqlLogger;
        _containerName = configuration["GtfsTargetContainerName"] ?? "gtfs-firenze-data";
    }

    public async Task<(bool Success, int RecordsProcessed, long ProcessingTimeMs, string? ErrorMessage)>
        RunAsync(string runId, CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        int recordsInserted = 0;
        await Log(runId, LogLevel.Information, "[START] Import stops.txt → daily_stops");

        try
        {
            // Pulizia preventiva per idempotenza
            // Cleanup e gestione FK gestiti centralmente da GtfsPipeline prima di questo step.

            // stop_ids univoci da daily_stop_times_trips (già popolata)
            var stopIdSet = new HashSet<string>(StringComparer.Ordinal);
            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync(ct);
                using var cmd = new SqlCommand(
                    "SELECT DISTINCT stop_id FROM daily_stop_times_trips WHERE stop_id IS NOT NULL", conn)
                    { CommandTimeout = 120 };
                using var rdr = await cmd.ExecuteReaderAsync(ct);
                while (await rdr.ReadAsync(ct))
                    stopIdSet.Add(rdr.GetString(0));
            }
            await Log(runId, LogLevel.Information, $"{stopIdSet.Count} stop_ids univoci in daily_stop_times_trips → lettura stops.txt...");

            var blobClient = _blobServiceClient
                .GetBlobContainerClient(_containerName)
                .GetBlobClient("stops.txt");

            if (!await blobClient.ExistsAsync(ct))
                throw new Exception($"Blob stops.txt non trovato nel container {_containerName}");

            int stopIdIdx = -1, codeIdx = -1, nameIdx = -1, descIdx = -1, latIdx = -1, lonIdx = -1;

            var batch = new List<(string StopId, string? Code, string? Name, string? Desc, double? Lat, double? Lon)>(BatchSize);

            using (var stream = await blobClient.OpenReadAsync(cancellationToken: ct))
            using (var reader = new StreamReader(stream))
            {
                var header = ParseCsvLine(await reader.ReadLineAsync() ?? "");
                for (int i = 0; i < header.Length; i++)
                {
                    var col = header[i].Trim().Trim('"').ToLower();
                    if (col == "stop_id")        stopIdIdx = i;
                    else if (col == "stop_code") codeIdx   = i;
                    else if (col == "stop_name") nameIdx   = i;
                    else if (col == "stop_desc") descIdx   = i;
                    else if (col == "stop_lat")  latIdx    = i;
                    else if (col == "stop_lon")  lonIdx    = i;
                }

                using var conn = new SqlConnection(_connectionString);
                await conn.OpenAsync(ct);

                string? line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    var p = ParseCsvLine(line);

                    var stopId = GetCol(p, stopIdIdx);
                    if (!stopIdSet.Contains(stopId!)) continue;

                    batch.Add((
                        stopId!,
                        GetCol(p, codeIdx),
                        GetCol(p, nameIdx),
                        GetCol(p, descIdx),
                        TryDouble(GetCol(p, latIdx)),
                        TryDouble(GetCol(p, lonIdx))
                    ));

                    if (batch.Count >= BatchSize)
                    {
                        recordsInserted += await FlushStopsBatchAsync(conn, batch, ct);
                        batch.Clear();
                        await Log(runId, LogLevel.Information, $"daily_stops: {recordsInserted} stops inserite finora");
                    }
                }

                if (batch.Count > 0)
                    recordsInserted += await FlushStopsBatchAsync(conn, batch, ct);
            }

            sw.Stop();
            await Log(runId, LogLevel.Information, $"[DONE] {recordsInserted} stops in {sw.ElapsedMilliseconds}ms");
            return (true, recordsInserted, sw.ElapsedMilliseconds, null);
        }
        catch (Exception ex)
        {
            sw.Stop();
            await Log(runId, LogLevel.Error, $"[ERROR] {ex.Message}", ex.ToString());
            return (false, recordsInserted, sw.ElapsedMilliseconds, ex.Message);
        }
    }

    /// <summary>
    /// geography::Point non è supportato da SqlBulkCopy: usa INSERT multi-row parametrizzato.
    /// Schema daily_stops: stop_id, stop_code, stop_name, stop_desc, location (geography)
    /// </summary>
    private static async Task<int> FlushStopsBatchAsync(
        SqlConnection conn,
        List<(string StopId, string? Code, string? Name, string? Desc, double? Lat, double? Lon)> batch,
        CancellationToken ct)
    {
        if (batch.Count == 0) return 0;

        var sb = new StringBuilder();
        sb.Append("INSERT INTO daily_stops (stop_id, stop_code, stop_name, stop_desc, location) VALUES ");

        var cmd = new SqlCommand { Connection = conn, CommandTimeout = 120 };

        for (int i = 0; i < batch.Count; i++)
        {
            var s = batch[i];
            if (i > 0) sb.Append(',');
            sb.Append($"(@sid{i},@sc{i},@sn{i},@sd{i},");
            if (s.Lat.HasValue && s.Lon.HasValue)
                sb.Append($"geography::Point(@lat{i},@lon{i},4326)");
            else
                sb.Append("NULL");
            sb.Append(')');

            cmd.Parameters.AddWithValue($"@sid{i}", s.StopId);
            cmd.Parameters.AddWithValue($"@sc{i}",  (object?)s.Code ?? DBNull.Value);
            cmd.Parameters.AddWithValue($"@sn{i}",  (object?)s.Name ?? DBNull.Value);
            cmd.Parameters.AddWithValue($"@sd{i}",  (object?)s.Desc ?? DBNull.Value);
            if (s.Lat.HasValue && s.Lon.HasValue)
            {
                cmd.Parameters.AddWithValue($"@lat{i}", s.Lat.Value);
                cmd.Parameters.AddWithValue($"@lon{i}", s.Lon.Value);
            }
        }

        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
        return batch.Count;
    }

    private static string[] ParseCsvLine(string line)
    {
        var fields = new List<string>();
        int pos = 0;
        while (pos <= line.Length)
        {
            if (pos < line.Length && line[pos] == '"')
            {
                pos++; // skip opening quote
                var sb = new StringBuilder();
                while (pos < line.Length)
                {
                    if (line[pos] == '"')
                    {
                        pos++;
                        if (pos < line.Length && line[pos] == '"') { sb.Append('"'); pos++; } // escaped ""
                        else break; // closing quote
                    }
                    else { sb.Append(line[pos++]); }
                }
                fields.Add(sb.ToString());
                if (pos < line.Length && line[pos] == ',') pos++;
            }
            else
            {
                int comma = line.IndexOf(',', pos);
                if (comma < 0) { fields.Add(line.Substring(pos)); break; }
                fields.Add(line.Substring(pos, comma - pos));
                pos = comma + 1;
            }
        }
        return fields.ToArray();
    }

    private static string? GetCol(string[] p, int idx) =>
        idx >= 0 && idx < p.Length ? p[idx].Trim().Trim('"') : null;

    private static double? TryDouble(string? s) =>
        double.TryParse(s, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : null;

    private Task Log(string runId, LogLevel level, string message, string? ex = null) =>
        _sqlLogger.LogAsync(level, nameof(ImportStopsStep), runId, message, ex);
}
