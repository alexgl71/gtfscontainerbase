using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using GtfsContainer.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.IO.Compression;
using System.Net.Http;

namespace GtfsContainer.Pipeline;

/// <summary>
/// Porta la logica di DownloadZipToBlobActivity + UnzipFromBlobToBlobsActivity + DeleteBlobActivity.
/// Scarica lo ZIP dal feed GTFS, lo salva in un blob temp, lo decomprime nel container target,
/// poi elimina il blob temp.
/// </summary>
public class DownloadAndUnzipStep
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly ISqlLoggingService _sqlLogger;
    private readonly string _zipDownloadUrl;
    private readonly string _tempContainerName;
    private readonly string _targetContainerName;

    public DownloadAndUnzipStep(
        IHttpClientFactory httpClientFactory,
        BlobServiceClient blobServiceClient,
        IConfiguration configuration,
        ISqlLoggingService sqlLogger)
    {
        _httpClientFactory = httpClientFactory;
        _blobServiceClient = blobServiceClient;
        _sqlLogger = sqlLogger;
        _zipDownloadUrl      = configuration["ZipDownloadUrl"]          ?? throw new InvalidOperationException("ZipDownloadUrl non configurato");
        _tempContainerName   = configuration["GtfsTempContainerName"]   ?? throw new InvalidOperationException("GtfsTempContainerName non configurato");
        _targetContainerName = configuration["GtfsTargetContainerName"] ?? throw new InvalidOperationException("GtfsTargetContainerName non configurato");
    }

    public async Task<List<string>> RunAsync(string runId, CancellationToken ct = default)
    {
        await Log(runId, LogLevel.Information, $"[START] Download ZIP da {_zipDownloadUrl}");

        // ── 1. Download ZIP → blob temp ───────────────────────────────────────
        var tempBlobUri = await DownloadZipToBlobAsync(runId, ct);
        if (string.IsNullOrEmpty(tempBlobUri))
            throw new Exception("Download ZIP fallito: blob temp non creato.");

        // ── 2. Unzip → container target ───────────────────────────────────────
        List<string> uploadedFiles;
        try
        {
            uploadedFiles = await UnzipToBlobsAsync(tempBlobUri, runId, ct);
        }
        finally
        {
            // ── 3. Elimina blob temp (anche in caso di errore) ────────────────
            await DeleteTempBlobAsync(tempBlobUri, runId);
        }

        await Log(runId, LogLevel.Information, $"[DONE] {uploadedFiles.Count} file nel container '{_targetContainerName}'");
        return uploadedFiles;
    }

    // ── Download ──────────────────────────────────────────────────────────────

    private async Task<string?> DownloadZipToBlobAsync(string runId, CancellationToken ct)
    {
        var client = _httpClientFactory.CreateClient("GtfsDownload");

        await Log(runId, LogLevel.Information, $"GET {_zipDownloadUrl}");
        var response = await client.GetAsync(_zipDownloadUrl, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();

        var tempContainer = _blobServiceClient.GetBlobContainerClient(_tempContainerName);
        await tempContainer.CreateIfNotExistsAsync(PublicAccessType.None, cancellationToken: ct);

        var blobName = $"gtfs-download-{Guid.NewGuid()}.zip";
        var blobClient = tempContainer.GetBlobClient(blobName);

        await Log(runId, LogLevel.Information, $"Upload ZIP → {_tempContainerName}/{blobName}");
        using var stream = await response.Content.ReadAsStreamAsync(ct);
        await blobClient.UploadAsync(stream, overwrite: true, cancellationToken: ct);

        await Log(runId, LogLevel.Information, $"ZIP caricato: {blobClient.Uri}");
        return blobClient.Uri.ToString();
    }

    // ── Unzip ─────────────────────────────────────────────────────────────────

    private async Task<List<string>> UnzipToBlobsAsync(string tempBlobUri, string runId, CancellationToken ct)
    {
        var uri = new Uri(tempBlobUri);
        var sourceContainer = uri.Segments[1].TrimEnd('/');
        var sourceBlobName  = string.Join("", uri.Segments[2..]).TrimEnd('/');

        var sourceBlobClient = _blobServiceClient
            .GetBlobContainerClient(sourceContainer)
            .GetBlobClient(sourceBlobName);

        if (!await sourceBlobClient.ExistsAsync(ct))
            throw new Exception($"Blob temp non trovato: {tempBlobUri}");

        var targetContainer = _blobServiceClient.GetBlobContainerClient(_targetContainerName);
        await targetContainer.CreateIfNotExistsAsync(PublicAccessType.None, cancellationToken: ct);

        // Pulisce il container target prima di caricare i nuovi file
        await Log(runId, LogLevel.Information, $"Pulizia container '{_targetContainerName}'...");
        int deletedCount = 0;
        await foreach (var item in targetContainer.GetBlobsAsync(cancellationToken: ct))
        {
            await targetContainer.GetBlobClient(item.Name).DeleteIfExistsAsync(cancellationToken: ct);
            deletedCount++;
        }
        await Log(runId, LogLevel.Information, $"Eliminati {deletedCount} file dal container target");

        // Scarica lo ZIP in memoria e decomprime
        await Log(runId, LogLevel.Information, "Download ZIP da blob temp in memoria...");
        using var zipStream = new MemoryStream();
        await sourceBlobClient.DownloadToAsync(zipStream, ct);
        zipStream.Position = 0;

        var uploadedFiles = new List<string>();
        using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, leaveOpen: false);

        await Log(runId, LogLevel.Information, $"Estrazione {archive.Entries.Count} file dallo ZIP...");
        foreach (var entry in archive.Entries)
        {
            if (string.IsNullOrEmpty(entry.Name) || entry.FullName.EndsWith('/') || entry.Length == 0)
                continue;

            var targetBlobName = Path.GetFileName(entry.FullName);
            var targetBlob = targetContainer.GetBlobClient(targetBlobName);

            using var entryStream = entry.Open();
            await targetBlob.UploadAsync(entryStream, overwrite: true, cancellationToken: ct);
            uploadedFiles.Add(targetBlobName);
        }

        await Log(runId, LogLevel.Information, $"Estrazione completata: {uploadedFiles.Count} file → '{_targetContainerName}'");
        return uploadedFiles;
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    private async Task DeleteTempBlobAsync(string tempBlobUri, string runId)
    {
        try
        {
            var uri = new Uri(tempBlobUri);
            var container = uri.Segments[1].TrimEnd('/');
            var blobName  = string.Join("", uri.Segments[2..]).TrimEnd('/');
            await _blobServiceClient
                .GetBlobContainerClient(container)
                .GetBlobClient(blobName)
                .DeleteIfExistsAsync();
            await Log(runId, LogLevel.Information, $"Blob temp eliminato: {blobName}");
        }
        catch (Exception ex)
        {
            await Log(runId, LogLevel.Warning, $"Impossibile eliminare blob temp: {ex.Message}");
        }
    }

    private Task Log(string runId, LogLevel level, string message) =>
        _sqlLogger.LogAsync(level, nameof(DownloadAndUnzipStep), runId, message);
}
