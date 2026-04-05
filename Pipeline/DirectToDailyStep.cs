using Azure.Storage.Blobs;
using GtfsContainer.Services;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Diagnostics;

namespace GtfsContainer.Pipeline;

public record DirectToDailyResult(
    bool Success,
    int ServiceIdsCount,
    int RouteIdsCount,
    int TripCount,
    int StopTimesCount,
    long ProcessingTimeMs,
    string? ErrorMessage);

/// <summary>
/// Porta la logica di FiDirectToDailyActivity.
/// Legge dal blob storage: calendar_dates → routes → trips → stop_times
/// e inserisce direttamente nelle daily tables senza passare per le base tables.
/// </summary>
public class DirectToDailyStep
{
    private readonly string _connectionString;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly ISqlLoggingService _sqlLogger;
    private readonly string _containerName;
    private readonly HashSet<string> _agencyIds;

    private const int BulkBatch = 50_000;

    public DirectToDailyStep(
        IConfiguration configuration,
        BlobServiceClient blobServiceClient,
        ISqlLoggingService sqlLogger)
    {
        _connectionString = configuration["SqlConnectionString"] ?? throw new InvalidOperationException("SqlConnectionString non configurato");
        _blobServiceClient = blobServiceClient;
        _sqlLogger = sqlLogger;
        _containerName = configuration["GtfsTargetContainerName"] ?? throw new InvalidOperationException("GtfsTargetContainerName non configurato");
        var agencyConfig = configuration["AgencyIds"] ?? configuration["AgencyId"]
            ?? throw new InvalidOperationException("AgencyId non configurato");
        _agencyIds = agencyConfig
            .Split(',')
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrEmpty(x))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public async Task<DirectToDailyResult> RunAsync(string runId, CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        var today = DateTime.Today.ToString("yyyyMMdd");
        await Log(runId, LogLevel.Information, $"[START] oggi={today} agencies={string.Join(",", _agencyIds)}");

        try
        {
            // Cleanup e gestione FK gestiti centralmente da GtfsPipeline prima di questo step.

            var container = _blobServiceClient.GetBlobContainerClient(_containerName);

            // ── 1. calendar_dates.txt → service_ids attivi oggi ──────────────
            await Log(runId, LogLevel.Information, "[1/4] Lettura calendar_dates.txt...");
            var serviceIds = await ReadServiceIdsAsync(container, today, ct);
            await Log(runId, LogLevel.Information, $"[1/4] {serviceIds.Count} service_ids attivi oggi");

            // ── 2. routes.txt → daily_routes ─────────────────────────────────
            await Log(runId, LogLevel.Information, "[2/4] Lettura routes.txt...");
            var routeIds = await ReadRoutesAndInsertDailyAsync(container, runId, ct);
            await Log(runId, LogLevel.Information, $"[2/4] {routeIds.Count} routes inserite in daily_routes");

            // ── 3. trips.txt → daily_trips + DailyTripStopTimes ──────────────
            await Log(runId, LogLevel.Information, $"[3/4] Lettura trips.txt ({routeIds.Count} routes × {serviceIds.Count} service_ids)...");
            var tripDict = await ReadTripsAndInsertDailyAsync(container, routeIds, serviceIds, runId, ct);
            await Log(runId, LogLevel.Information, $"[3/4] {tripDict.Count} trips in daily_trips + DailyTripStopTimes");

            // ── 4. stop_times.txt → daily_stop_times_trips + daily_stop_times ─
            await Log(runId, LogLevel.Information, $"[4/4] Lettura stop_times.txt ({tripDict.Count} trip_ids, batch {BulkBatch:N0})...");
            int stopTimesCount = await ReadStopTimesAndInsertDailyAsync(container, tripDict, runId, ct);
            await Log(runId, LogLevel.Information, $"[4/4] {stopTimesCount:N0} righe in daily_stop_times_trips + daily_stop_times");

            sw.Stop();
            var summary = $"[DONE] {sw.ElapsedMilliseconds}ms | service_ids={serviceIds.Count} routes={routeIds.Count} trips={tripDict.Count} stop_times={stopTimesCount:N0}";
            await Log(runId, LogLevel.Information, summary);

            return new DirectToDailyResult(true, serviceIds.Count, routeIds.Count, tripDict.Count, stopTimesCount, sw.ElapsedMilliseconds, null);
        }
        catch (Exception ex)
        {
            sw.Stop();
            await Log(runId, LogLevel.Error, $"[ERROR] {ex.Message}", ex.ToString());
            return new DirectToDailyResult(false, 0, 0, 0, 0, sw.ElapsedMilliseconds, ex.Message);
        }
    }

    // ── Step 1: calendar_dates ────────────────────────────────────────────────

    private static async Task<HashSet<string>> ReadServiceIdsAsync(
        BlobContainerClient container, string today, CancellationToken ct)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        var blob = container.GetBlobClient("calendar_dates.txt");
        if (!await blob.ExistsAsync(ct))
            throw new Exception("Blob calendar_dates.txt non trovato");

        int serviceIdx = -1, dateIdx = -1, exceptionIdx = -1;
        using var stream = await blob.OpenReadAsync(cancellationToken: ct);
        using var reader = new StreamReader(stream);

        var header = ParseCsvLine(await reader.ReadLineAsync() ?? "");
        for (int i = 0; i < header.Length; i++)
        {
            var col = header[i].Trim().Trim('"').ToLower();
            if (col == "service_id")          serviceIdx   = i;
            else if (col == "date")           dateIdx      = i;
            else if (col == "exception_type") exceptionIdx = i;
        }

        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            var p = ParseCsvLine(line);
            if (GetCol(p, dateIdx) != today) continue;
            if (GetCol(p, exceptionIdx) != "1") continue;
            var sid = GetCol(p, serviceIdx);
            if (!string.IsNullOrEmpty(sid)) set.Add(sid);
        }
        return set;
    }

    // ── Step 2: routes ────────────────────────────────────────────────────────

    private async Task<HashSet<string>> ReadRoutesAndInsertDailyAsync(
        BlobContainerClient container, string runId, CancellationToken ct)
    {
        var routeIds = new HashSet<string>(StringComparer.Ordinal);
        var blob = container.GetBlobClient("routes.txt");
        if (!await blob.ExistsAsync(ct))
            throw new Exception("Blob routes.txt non trovato");

        int routeIdx = -1, agencyIdx = -1, shortNameIdx = -1, longNameIdx = -1,
            descIdx = -1, typeIdx = -1, sortIdx = -1;

        using var stream = await blob.OpenReadAsync(cancellationToken: ct);
        using var reader = new StreamReader(stream);

        var header = ParseCsvLine(await reader.ReadLineAsync() ?? "");
        for (int i = 0; i < header.Length; i++)
        {
            var col = header[i].Trim().Trim('"').ToLower();
            if (col == "route_id")              routeIdx     = i;
            else if (col == "agency_id")        agencyIdx    = i;
            else if (col == "route_short_name") shortNameIdx = i;
            else if (col == "route_long_name")  longNameIdx  = i;
            else if (col == "route_desc")       descIdx      = i;
            else if (col == "route_type")       typeIdx      = i;
            else if (col == "route_sort_order") sortIdx      = i;
        }

        var dt = new DataTable();
        dt.Columns.Add("route_id",         typeof(string));
        dt.Columns.Add("route_short_name", typeof(string));
        dt.Columns.Add("route_long_name",  typeof(string));
        dt.Columns.Add("route_desc",       typeof(string));
        dt.Columns.Add("route_type",       typeof(int));
        dt.Columns.Add("route_sort_order", typeof(int));

        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            var p = ParseCsvLine(line);
            if (!_agencyIds.Contains(GetCol(p, agencyIdx) ?? string.Empty)) continue;
            var rid = GetCol(p, routeIdx);
            if (string.IsNullOrEmpty(rid)) continue;

            routeIds.Add(rid);
            var row = dt.NewRow();
            row["route_id"]         = rid;
            row["route_short_name"] = (object?)GetCol(p, shortNameIdx) ?? DBNull.Value;
            row["route_long_name"]  = (object?)GetCol(p, longNameIdx)  ?? DBNull.Value;
            row["route_desc"]       = (object?)GetCol(p, descIdx)      ?? DBNull.Value;
            row["route_type"]       = (object?)TryInt(GetCol(p, typeIdx)) ?? DBNull.Value;
            row["route_sort_order"] = (object?)TryInt(GetCol(p, sortIdx)) ?? DBNull.Value;
            dt.Rows.Add(row);
        }

        await Log(runId, LogLevel.Information, $"[2/4] {dt.Rows.Count} routes [{string.Join(",", _agencyIds)}] → bulk insert daily_routes...");
        using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await BulkInsertAsync(conn, "daily_routes", dt, ct);
        return routeIds;
    }

    // ── Step 3: trips ─────────────────────────────────────────────────────────

    private async Task<Dictionary<string, (string RouteId, string? Headsign, string? ShapeId, int? DirectionId)>>
        ReadTripsAndInsertDailyAsync(
            BlobContainerClient container,
            HashSet<string> routeIds,
            HashSet<string> serviceIds,
            string runId,
            CancellationToken ct)
    {
        var dict = new Dictionary<string, (string RouteId, string? Headsign, string? ShapeId, int? DirectionId)>(StringComparer.Ordinal);
        var blob = container.GetBlobClient("trips.txt");
        if (!await blob.ExistsAsync(ct))
            throw new Exception("Blob trips.txt non trovato");

        int routeIdx = -1, serviceIdx = -1, tripIdx = -1,
            headsignIdx = -1, shapeIdx = -1, dirIdx = -1;

        using var stream = await blob.OpenReadAsync(cancellationToken: ct);
        using var reader = new StreamReader(stream);

        var header = ParseCsvLine(await reader.ReadLineAsync() ?? "");
        for (int i = 0; i < header.Length; i++)
        {
            var col = header[i].Trim().Trim('"').ToLower();
            if (col == "route_id")           routeIdx    = i;
            else if (col == "service_id")    serviceIdx  = i;
            else if (col == "trip_id")       tripIdx     = i;
            else if (col == "trip_headsign") headsignIdx = i;
            else if (col == "shape_id")      shapeIdx    = i;
            else if (col == "direction_id")  dirIdx      = i;
        }

        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            var p = ParseCsvLine(line);
            var routeId = GetCol(p, routeIdx);
            if (!routeIds.Contains(routeId!)) continue;
            if (!serviceIds.Contains(GetCol(p, serviceIdx)!)) continue;
            var tripId = GetCol(p, tripIdx);
            if (string.IsNullOrEmpty(tripId)) continue;
            dict[tripId] = (routeId!, GetCol(p, headsignIdx), GetCol(p, shapeIdx), TryInt(GetCol(p, dirIdx)));
        }

        var dtTrips = new DataTable();
        dtTrips.Columns.Add("route_id",      typeof(string));
        dtTrips.Columns.Add("trip_id",        typeof(string));
        dtTrips.Columns.Add("trip_headsign",  typeof(string));
        dtTrips.Columns.Add("shape_id",       typeof(string));

        var dtDTST = new DataTable();
        dtDTST.Columns.Add("trip_id",  typeof(string));
        dtDTST.Columns.Add("delay",    typeof(int));
        dtDTST.Columns.Add("sequence", typeof(int));

        foreach (var kv in dict)
        {
            var r1 = dtTrips.NewRow();
            r1["route_id"]      = kv.Value.RouteId;
            r1["trip_id"]       = kv.Key;
            r1["trip_headsign"] = (object?)kv.Value.Headsign ?? DBNull.Value;
            r1["shape_id"]      = (object?)kv.Value.ShapeId  ?? DBNull.Value;
            dtTrips.Rows.Add(r1);

            var r2 = dtDTST.NewRow();
            r2["trip_id"]  = kv.Key;
            r2["delay"]    = DBNull.Value;
            r2["sequence"] = DBNull.Value;
            dtDTST.Rows.Add(r2);
        }

        using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await Log(runId, LogLevel.Information, $"[3/4] {dict.Count} trips → bulk insert daily_trips...");
        await BulkInsertAsync(conn, "daily_trips", dtTrips, ct);
        await Log(runId, LogLevel.Information, "[3/4] daily_trips OK → bulk insert DailyTripStopTimes...");
        await BulkInsertAsync(conn, "DailyTripStopTimes", dtDTST, ct);

        return dict;
    }

    // ── Step 4: stop_times ────────────────────────────────────────────────────

    private async Task<int> ReadStopTimesAndInsertDailyAsync(
        BlobContainerClient container,
        Dictionary<string, (string RouteId, string? Headsign, string? ShapeId, int? DirectionId)> tripDict,
        string runId,
        CancellationToken ct)
    {
        var blob = container.GetBlobClient("stop_times.txt");
        if (!await blob.ExistsAsync(ct))
            throw new Exception("Blob stop_times.txt non trovato");

        int tripIdx = -1, arrIdx = -1, stopIdIdx = -1, seqIdx = -1;
        using var stream = await blob.OpenReadAsync(cancellationToken: ct);
        using var reader = new StreamReader(stream);

        var header = ParseCsvLine(await reader.ReadLineAsync() ?? "");
        for (int i = 0; i < header.Length; i++)
        {
            var col = header[i].Trim().Trim('"').ToLower();
            if (col == "trip_id")            tripIdx   = i;
            else if (col == "arrival_time")  arrIdx    = i;
            else if (col == "stop_id")       stopIdIdx = i;
            else if (col == "stop_sequence") seqIdx    = i;
        }

        using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        var dtTrips = new DataTable();
        dtTrips.Columns.Add("trip_id",       typeof(string));
        dtTrips.Columns.Add("arrival_time",  typeof(int));
        dtTrips.Columns.Add("stop_id",       typeof(string));
        dtTrips.Columns.Add("stop_sequence", typeof(int));
        dtTrips.Columns.Add("route_id",      typeof(string));
        dtTrips.Columns.Add("trip_headsign", typeof(string));
        dtTrips.Columns.Add("shape_id",      typeof(string));
        dtTrips.Columns.Add("direction_id",  typeof(int));

        var dtST = new DataTable();
        dtST.Columns.Add("trip_id",       typeof(string));
        dtST.Columns.Add("arrival_time",  typeof(int));
        dtST.Columns.Add("stop_id",       typeof(string));
        dtST.Columns.Add("stop_sequence", typeof(int));

        int total = 0, batchNum = 0;
        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            var p = ParseCsvLine(line);

            var tripId = GetCol(p, tripIdx);
            if (!tripDict.TryGetValue(tripId!, out var trip)) continue;
            if (string.IsNullOrEmpty(trip.RouteId)) continue;

            var arrStr = GetCol(p, arrIdx);
            if (string.IsNullOrEmpty(arrStr) || arrStr.Length != 8) continue;

            var stopId = GetCol(p, stopIdIdx);
            if (string.IsNullOrEmpty(stopId)) continue;

            var seq = TryInt(GetCol(p, seqIdx)) ?? 0;
            var arrSec = ParseTimeToSeconds(arrStr);

            var rowTrips = dtTrips.NewRow();
            rowTrips["trip_id"]       = tripId!;
            rowTrips["arrival_time"]  = arrSec;
            rowTrips["stop_id"]       = stopId;
            rowTrips["stop_sequence"] = seq;
            rowTrips["route_id"]      = trip.RouteId;
            rowTrips["trip_headsign"] = (object?)trip.Headsign   ?? DBNull.Value;
            rowTrips["shape_id"]      = (object?)trip.ShapeId    ?? DBNull.Value;
            rowTrips["direction_id"]  = (object?)trip.DirectionId ?? DBNull.Value;
            dtTrips.Rows.Add(rowTrips);

            var rowST = dtST.NewRow();
            rowST["trip_id"]       = tripId!;
            rowST["arrival_time"]  = arrSec;
            rowST["stop_id"]       = stopId;
            rowST["stop_sequence"] = seq;
            dtST.Rows.Add(rowST);

            total++;

            if (dtTrips.Rows.Count >= BulkBatch)
            {
                batchNum++;
                await BulkInsertAsync(conn, "daily_stop_times_trips", dtTrips, ct);
                await BulkInsertAsync(conn, "daily_stop_times", dtST, ct);
                dtTrips.Clear();
                dtST.Clear();
                await Log(runId, LogLevel.Information, $"[4/4] batch #{batchNum} → {total:N0} righe totali");
            }
        }

        if (dtTrips.Rows.Count > 0)
        {
            batchNum++;
            await BulkInsertAsync(conn, "daily_stop_times_trips", dtTrips, ct);
            await BulkInsertAsync(conn, "daily_stop_times", dtST, ct);
            await Log(runId, LogLevel.Information, $"[4/4] batch #{batchNum} (finale) → {total:N0} righe totali");
        }

        return total;
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static async Task BulkInsertAsync(SqlConnection conn, string table, DataTable dt, CancellationToken ct)
    {
        using var bulk = new SqlBulkCopy(conn)
        {
            DestinationTableName = table,
            BulkCopyTimeout = 300,
            BatchSize = BulkBatch
        };
        foreach (DataColumn col in dt.Columns)
            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
        await bulk.WriteToServerAsync(dt, ct);
    }

    private static int ParseTimeToSeconds(string t) =>
        ((t[0] - '0') * 10 + (t[1] - '0')) * 3600 +
        ((t[3] - '0') * 10 + (t[4] - '0')) * 60 +
        ((t[6] - '0') * 10 + (t[7] - '0'));

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

    private Task Log(string runId, LogLevel level, string message, string? ex = null) =>
        _sqlLogger.LogAsync(level, nameof(DirectToDailyStep), runId, message, ex);
}
