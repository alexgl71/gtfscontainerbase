# ── Build stage ───────────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY GtfsContainer.csproj .
RUN dotnet restore

COPY . .
RUN dotnet publish GtfsContainer.csproj -c Release -o /app/publish --no-restore

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app

COPY --from=build /app/publish .

# Variabili di configurazione attese (passate via env vars su Azure Container Apps)
# - Storage                  : Azure Blob Storage connection string
# - SqlConnectionString      : Azure SQL connection string (DB GTFSFI)
# - ZipDownloadUrl           : URL del feed GTFS statico (.zip)
# - GtfsTempContainerName    : default "gtfs-fi-temp"
# - GtfsTargetContainerName  : default "gtfs-firenze-data"
# - AgencyId                 : default "UFI"
# - ScheduleHourUtc          : ora UTC di esecuzione giornaliera (default 7)
# - RUN_ON_STARTUP           : "true" per eseguire subito all'avvio (test)

ENTRYPOINT ["dotnet", "GtfsContainer.dll"]
