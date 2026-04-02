using GtfsContainer.Services;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GtfsContainer.Pipeline;

/// <summary>
/// Esegue la pipeline di import GTFS Firenze in sequenza.
///
/// Gestione FK centralizzata:
///   - All'inizio: disabilita TUTTI i FK su TUTTE le tabelle daily, poi svuota tutto.
///   - Alla fine: riabilita i FK con validazione completa (WITH CHECK).
///   I singoli step NON gestiscono FK né cleanup: si occupano solo dell'insert.
///
/// Step:
///   0. Download ZIP + unzip → blob storage (gtfs-firenze-data)
///   1. DirectToDaily: calendar_dates → routes → trips → stop_times → daily tables
///   2. ImportStops: stops.txt → daily_stops
///   3. ImportShapes: shapes.txt → daily_shapes
/// </summary>
public class GtfsPipeline
{
    private readonly DownloadAndUnzipStep _downloadUnzip;
    private readonly DirectToDailyStep _directToDaily;
    private readonly ImportStopsStep _importStops;
    private readonly ImportShapesStep _importShapes;
    private readonly ISqlLoggingService _sqlLogger;
    private readonly string _connectionString;

    // Tabelle daily in ordine FK-safe (figli prima dei padri) per DELETE
    private static readonly string[] DailyTables =
    [
        "daily_stop_times_trips",
        "daily_stop_times",
        "DailyTripStopTimes",
        "daily_trips",
        "daily_stops",
        "daily_routes",
        "daily_shapes"
    ];

    public GtfsPipeline(
        DownloadAndUnzipStep downloadUnzip,
        DirectToDailyStep directToDaily,
        ImportStopsStep importStops,
        ImportShapesStep importShapes,
        ISqlLoggingService sqlLogger,
        IConfiguration configuration)
    {
        _downloadUnzip = downloadUnzip;
        _directToDaily = directToDaily;
        _importStops   = importStops;
        _importShapes  = importShapes;
        _sqlLogger     = sqlLogger;
        _connectionString = configuration["SqlConnectionString"]
            ?? throw new InvalidOperationException("SqlConnectionString non configurato");
    }

    public async Task RunAsync(CancellationToken ct = default)
    {
        var runId = Guid.NewGuid().ToString("N")[..12];
        await Log(runId, LogLevel.Information, $"========== PIPELINE START runId={runId} ==========");

        // ── Step 0: Download ZIP + unzip ──────────────────────────────────────
        await Log(runId, LogLevel.Information, "Step 0: Download ZIP e unzip nel blob storage");
        var files = await _downloadUnzip.RunAsync(runId, ct);
        await Log(runId, LogLevel.Information, $"Step 0 OK: {files.Count} file nel blob storage");

        // ── Pre-import: disabilita FK e svuota tutte le tabelle daily ─────────
        await Log(runId, LogLevel.Information, "Pre-import: disable FK + clear all daily tables");
        await DisableFkAndClearAllAsync(runId, ct);
        await Log(runId, LogLevel.Information, "Pre-import OK: tabelle daily svuotate, FK disabilitati");

        // ── Step 1: blob → daily tables ───────────────────────────────────────
        await Log(runId, LogLevel.Information, "Step 1: import diretto da blob → tabelle daily");
        var dailyResult = await _directToDaily.RunAsync(runId, ct);
        if (!dailyResult.Success)
            throw new Exception($"DirectToDailyStep fallita: {dailyResult.ErrorMessage}");
        await Log(runId, LogLevel.Information,
            $"Step 1 OK: {dailyResult.ServiceIdsCount} service_ids, {dailyResult.RouteIdsCount} routes, " +
            $"{dailyResult.TripCount} trips, {dailyResult.StopTimesCount:N0} stop_times in {dailyResult.ProcessingTimeMs}ms");

        // ── Step 2: stops ─────────────────────────────────────────────────────
        await Log(runId, LogLevel.Information, "Step 2: import stops");
        var (stopsOk, stopsCount, stopsMs, stopsErr) = await _importStops.RunAsync(runId, ct);
        if (!stopsOk)
            throw new Exception($"ImportStopsStep fallita: {stopsErr}");
        await Log(runId, LogLevel.Information, $"Step 2 OK: {stopsCount} stops in {stopsMs}ms");

        // ── Step 3: shapes ────────────────────────────────────────────────────
        await Log(runId, LogLevel.Information, "Step 3: import shapes");
        var (shapesOk, shapesCount, shapesMs, shapesErr) = await _importShapes.RunAsync(runId, ct);
        if (!shapesOk)
            throw new Exception($"ImportShapesStep fallita: {shapesErr}");
        await Log(runId, LogLevel.Information, $"Step 3 OK: {shapesCount:N0} shape points in {shapesMs}ms");

        // ── Post-import: riabilita e valida tutti i FK ────────────────────────
        await Log(runId, LogLevel.Information, "Post-import: re-enable FK con validazione completa");
        await ReEnableFkAsync(runId, ct);
        await Log(runId, LogLevel.Information, "Post-import OK: FK riabilitati e validati");

        var summary =
            $"service_ids={dailyResult.ServiceIdsCount} routes={dailyResult.RouteIdsCount} " +
            $"trips={dailyResult.TripCount} stop_times={dailyResult.StopTimesCount:N0} " +
            $"stops={stopsCount} shape_points={shapesCount:N0}";
        await Log(runId, LogLevel.Information, $"========== PIPELINE DONE: {summary} ==========");
    }

    // ── FK + Clear ────────────────────────────────────────────────────────────

    private async Task DisableFkAndClearAllAsync(string runId, CancellationToken ct)
    {
        using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // 1. Disabilita tutti i FK constraint su tutte le tabelle daily
        var disableSql = string.Join("\n", DailyTables.Select(t => $"ALTER TABLE {t} NOCHECK CONSTRAINT ALL;"));
        using (var cmd = new SqlCommand(disableSql, conn) { CommandTimeout = 60 })
            await cmd.ExecuteNonQueryAsync(ct);
        await Log(runId, LogLevel.Information, "FK disabilitati su tutte le tabelle daily");

        // 2. Svuota ogni tabella: prova TRUNCATE (istantaneo), fallback DELETE
        // Nota: TRUNCATE fallisce per le tabelle parent (referenziate da FK) anche con FK disabilitati.
        // DELETE funziona sempre, ed è più veloce senza FK check attivi.
        foreach (var table in DailyTables)
        {
            try
            {
                using var cmd = new SqlCommand($"TRUNCATE TABLE {table}", conn) { CommandTimeout = 120 };
                await cmd.ExecuteNonQueryAsync(ct);
                await Log(runId, LogLevel.Information, $"[CLEAR] {table}: TRUNCATE OK");
            }
            catch
            {
                using var cmd = new SqlCommand($"DELETE FROM {table}", conn) { CommandTimeout = 600 };
                await cmd.ExecuteNonQueryAsync(ct);
                await Log(runId, LogLevel.Information, $"[CLEAR] {table}: DELETE OK (TRUNCATE non consentito - tabella referenziata)");
            }
        }
    }

    private async Task ReEnableFkAsync(string runId, CancellationToken ct)
    {
        using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // WITH CHECK CHECK CONSTRAINT: riabilita E valida tutti i dati esistenti.
        // Se la validazione fallisce significa che i dati inseriti sono inconsistenti → errore reale.
        var enableSql = string.Join("\n", DailyTables.Select(t => $"ALTER TABLE {t} WITH CHECK CHECK CONSTRAINT ALL;"));
        using var cmd = new SqlCommand(enableSql, conn) { CommandTimeout = 120 };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private Task Log(string runId, LogLevel level, string message) =>
        _sqlLogger.LogAsync(level, nameof(GtfsPipeline), runId, message);
}
