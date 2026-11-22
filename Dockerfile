# Global args: must precede the first FROM to be usable in FROM ${BASE_IMAGE}.
ARG BASE_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0

FROM mcr.microsoft.com/dotnet/sdk:10.0.401 AS build
WORKDIR /src

COPY ["global.json", "Directory.Build.props", "Directory.Packages.props", "NuGet.Config", ".version", "./"]
COPY ["src/RelayPackage.props", "src/"]
COPY ["src/Pernix.OutboxRelay.Host/Pernix.OutboxRelay.Host.csproj", "src/Pernix.OutboxRelay.Host/"]
COPY ["src/Pernix.OutboxRelay.Hosting/Pernix.OutboxRelay.Hosting.csproj", "src/Pernix.OutboxRelay.Hosting/"]
COPY ["src/Pernix.OutboxRelay.Core/Pernix.OutboxRelay.Core.csproj", "src/Pernix.OutboxRelay.Core/"]
COPY ["src/Pernix.OutboxRelay.Outbox.PostgreSql/Pernix.OutboxRelay.Outbox.PostgreSql.csproj", "src/Pernix.OutboxRelay.Outbox.PostgreSql/"]
COPY ["src/Pernix.OutboxRelay.Destination.Kafka/Pernix.OutboxRelay.Destination.Kafka.csproj", "src/Pernix.OutboxRelay.Destination.Kafka/"]

RUN dotnet restore src/Pernix.OutboxRelay.Host/Pernix.OutboxRelay.Host.csproj

COPY ["src/", "src/"]
RUN dotnet publish src/Pernix.OutboxRelay.Host/Pernix.OutboxRelay.Host.csproj -c Release --no-restore -o /app/publish

FROM ${BASE_IMAGE} AS final

ARG BUILD_VERSION=1.0.0
ARG BUILD_COMMIT=unknown

ENV SERVICE_NAME=outbox-relay \
    SERVICE_VERSION=$BUILD_VERSION \
    SERVICE_COMMIT=$BUILD_COMMIT \
    KESTREL__ENDPOINTS__DIAGNOSTICS__URL=http://+:5612

LABEL org.opencontainers.image.revision=$BUILD_COMMIT \
      org.opencontainers.image.version=$BUILD_VERSION \
      org.opencontainers.image.title=outbox-relay

EXPOSE 5612
WORKDIR /app
COPY --from=build --chown=1654:1654 /app/publish .
USER 1654:1654

# Exec form so SIGTERM reaches dotnet as PID 1.
ENTRYPOINT ["dotnet", "Pernix.OutboxRelay.Host.dll"]
