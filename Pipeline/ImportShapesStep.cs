using Azure.Storage.Blobs;
using GtfsContainer.Services;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Diagnostics;

namespace GtfsContainer.Pipeline;

/// <summary>
/// Porta la logica di FiImportShapesActivity.
/// Legge shapes.txt dal blob, filtra sugli shape_id usati in daily_stop_times_trips,
/// inserisce in daily_shapes con bulk copy.
/// </summary>
public class ImportShapesStep
{
    private readonly string _connectionString;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly ISqlLoggingService _sqlLogger;
    private readonly string _containerName;

    private const int BatchSize = 10_000;

    public ImportShapesStep(
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
        await Log(runId, LogLevel.Information, "[START] Import shapes.txt → daily_shapes");

        try
        {
            // Cleanup e gestione FK gestiti centralmente da GtfsPipeline prima di questo step.

            // shape_ids univoci da daily_stop_times_trips
            var shapeIdSet = new HashSet<string>(StringComparer.Ordinal);
            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync(ct);
                using var cmd = new SqlCommand(
                    "SELECT DISTINCT shape_id FROM daily_stop_times_trips WHERE shape_id IS NOT NULL AND shape_id <> ''",
                    conn) { CommandTimeout = 120 };
                using var rdr = await cmd.ExecuteReaderAsync(ct);
                while (await rdr.ReadAsync(ct))
                    shapeIdSet.Add(rdr.GetString(0));
            }
            await Log(runId, LogLevel.Information, $"{shapeIdSet.Count} shape_ids univoci → lettura shapes.txt...");

            var blobClient = _blobServiceClient
                .GetBlobContainerClient(_containerName)
                .GetBlobClient("shapes.txt");

            if (!await blobClient.ExistsAsync(ct))
                throw new Exception($"Blob shapes.txt non trovato nel container {_containerName}");

            int shapeIdIdx = -1, latIdx = -1, lonIdx = -1, seqIdx = -1;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);

            var batch = new DataTable();
            batch.Columns.Add("shape_id",          typeof(string));
            batch.Columns.Add("shape_pt_lat",      typeof(double));
            batch.Columns.Add("shape_pt_lon",      typeof(double));
            batch.Columns.Add("shape_pt_sequence", typeof(int));

            using (var stream = await blobClient.OpenReadAsync(cancellationToken: ct))
            using (var reader = new StreamReader(stream))
            {
                var header = ParseCsvLine(await reader.ReadLineAsync() ?? "");
                for (int i = 0; i < header.Length; i++)
                {
                    var col = header[i].Trim().Trim('"').ToLower();
                    if (col == "shape_id")               shapeIdIdx = i;
                    else if (col == "shape_pt_lat")      latIdx     = i;
                    else if (col == "shape_pt_lon")      lonIdx     = i;
                    else if (col == "shape_pt_sequence") seqIdx     = i;
                }

                string? line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    var p = ParseCsvLine(line);

                    var shapeId = GetCol(p, shapeIdIdx);
                    if (!shapeIdSet.Contains(shapeId!)) continue;

                    var lat = TryDouble(GetCol(p, latIdx));
                    var lon = TryDouble(GetCol(p, lonIdx));
                    var seq = TryInt(GetCol(p, seqIdx));
                    if (lat == null || lon == null || seq == null) continue;

                    var row = batch.NewRow();
                    row["shape_id"]          = shapeId!;
                    row["shape_pt_lat"]      = lat.Value;
                    row["shape_pt_lon"]      = lon.Value;
                    row["shape_pt_sequence"] = seq.Value;
                    batch.Rows.Add(row);

                    if (batch.Rows.Count >= BatchSize)
                    {
                        recordsInserted += await FlushBatchAsync(connection, batch, ct);
                        batch.Clear();
                        await Log(runId, LogLevel.Information, $"daily_shapes: {recordsInserted:N0} punti inseriti finora");
                    }
                }
            }

            if (batch.Rows.Count > 0)
                recordsInserted += await FlushBatchAsync(connection, batch, ct);

            sw.Stop();
            await Log(runId, LogLevel.Information, $"[DONE] {recordsInserted:N0} shape points in {sw.ElapsedMilliseconds}ms");
            return (true, recordsInserted, sw.ElapsedMilliseconds, null);
        }
        catch (Exception ex)
        {
            sw.Stop();
            await Log(runId, LogLevel.Error, $"[ERROR] {ex.Message}", ex.ToString());
            return (false, recordsInserted, sw.ElapsedMilliseconds, ex.Message);
        }
    }

    private static async Task<int> FlushBatchAsync(SqlConnection conn, DataTable dt, CancellationToken ct)
    {
        using var bulk = new SqlBulkCopy(conn)
        {
            DestinationTableName = "daily_shapes",
            BulkCopyTimeout = 300
        };
        foreach (DataColumn col in dt.Columns)
            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
        await bulk.WriteToServerAsync(dt, ct);
        return dt.Rows.Count;
    }

    private static string[] ParseCsvLine(string line)
    {
        var fields = new List<string>();
        int pos = 0;
        while (pos <= line.Length)
        {
            if (pos < line.Length && line[pos] == '"')
            {
                pos++;
                var sb = new System.Text.StringBuilder();
                while (pos < line.Length)
                {
                    if (line[pos] == '"')
                    {
                        pos++;
                        if (pos < line.Length && line[pos] == '"') { sb.Append('"'); pos++; }
                        else break;
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

    private static int? TryInt(string? s) =>
        int.TryParse(s, out var v) ? v : null;

    private static double? TryDouble(string? s) =>
        double.TryParse(s, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : null;

    private Task Log(string runId, LogLevel level, string message, string? ex = null) =>
        _sqlLogger.LogAsync(level, nameof(ImportShapesStep), runId, message, ex);
}
