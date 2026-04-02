using Azure.Storage.Blobs;
using GtfsContainer;
using GtfsContainer.Pipeline;
using GtfsContainer.Services;

// Cultura invariante per il parsing CSV (numeri decimali con punto)
System.Globalization.CultureInfo.DefaultThreadCurrentCulture =
    System.Globalization.CultureInfo.InvariantCulture;
System.Globalization.CultureInfo.DefaultThreadCurrentUICulture =
    System.Globalization.CultureInfo.InvariantCulture;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        // HTTP client con timeout lungo per lo ZIP Toscana
        services.AddHttpClient("GtfsDownload", client =>
        {
            client.Timeout = TimeSpan.FromMinutes(15);
            client.DefaultRequestHeaders.Add("User-Agent", "GtfsContainer/1.0");
        });

        // Azure Blob Storage
        services.AddSingleton(_ =>
        {
            var connStr = context.Configuration["Storage"]
                ?? throw new InvalidOperationException("Variabile 'Storage' (Azure Blob connection string) non configurata.");
            return new BlobServiceClient(connStr);
        });

        // Logging su SQL (scrive su GtfsImportLogs, stesso DB di GTFSFI)
        services.AddSingleton<ISqlLoggingService, SqlLoggingService>();

        // Pipeline steps
        services.AddTransient<DownloadAndUnzipStep>();
        services.AddTransient<DirectToDailyStep>();
        services.AddTransient<ImportStopsStep>();
        services.AddTransient<ImportShapesStep>();
        services.AddTransient<GtfsPipeline>();

        // Background worker con scheduling
        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();
