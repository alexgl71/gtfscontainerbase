using GtfsContainer.Pipeline;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace GtfsContainer;

public class Worker : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<Worker> _logger;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly int _scheduleHourUtc;
    private readonly bool _runOnStartup;

    public Worker(
        IServiceProvider serviceProvider,
        ILogger<Worker> logger,
        IHostApplicationLifetime lifetime,
        IConfiguration config)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _lifetime = lifetime;
        _scheduleHourUtc = int.TryParse(config["ScheduleHourUtc"], out var h) ? h : 7;
        _runOnStartup = config["RUN_ON_STARTUP"]?.Equals("true", StringComparison.OrdinalIgnoreCase) == true;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_runOnStartup)
        {
            // Modalità Container Apps Job: esegui la pipeline e termina il processo.
            _logger.LogInformation("RUN_ON_STARTUP=true → esecuzione pipeline e uscita");
            await RunPipelineAsync(stoppingToken);
            _lifetime.StopApplication();
            return;
        }

        // Modalità long-running (Worker Service standalone): aspetta lo schedule giornaliero
        while (!stoppingToken.IsCancellationRequested)
        {
            var next = NextScheduledRun();
            var delay = next - DateTime.UtcNow;
            _logger.LogInformation("Prossima esecuzione: {Next:u} UTC (tra {H}h {M}m)", next, (int)delay.TotalHours, delay.Minutes);

            try
            {
                await Task.Delay(delay, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }

            await RunPipelineAsync(stoppingToken);
        }
    }

    private DateTime NextScheduledRun()
    {
        var now = DateTime.UtcNow;
        var candidate = now.Date.AddHours(_scheduleHourUtc);
        return candidate > now ? candidate : candidate.AddDays(1);
    }

    private async Task RunPipelineAsync(CancellationToken ct)
    {
        using var scope = _serviceProvider.CreateScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<GtfsPipeline>();
        try
        {
            await pipeline.RunAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Pipeline fallita: {Message}", ex.Message);
        }
    }
}
