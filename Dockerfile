FROM mcr.microsoft.com/dotnet/sdk:10.0

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Sao_Paulo

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        perl \
        unzip \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
        | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_24.x nodistro main" \
        > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install -g wrangler \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64|arm64) rclone_arch="$arch" ;; \
      *) echo "Unsupported architecture for rclone: $arch" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://downloads.rclone.org/rclone-current-linux-${rclone_arch}.zip" -o /tmp/rclone.zip; \
    unzip -q /tmp/rclone.zip -d /tmp; \
    install -m 0755 /tmp/rclone-*-linux-${rclone_arch}/rclone /usr/local/bin/rclone; \
    ln -sf /usr/local/bin/rclone /usr/bin/rclone; \
    rm -rf /tmp/rclone.zip /tmp/rclone-*-linux-${rclone_arch}; \
    rclone version

WORKDIR /app

COPY . .

RUN chmod +x /app/src/scripts/deploy.sh /app/src/scripts/docker-entrypoint.sh \
    && dotnet restore /app/src/ETL/Processor/CNPJExporter.csproj

CMD ["/app/src/scripts/docker-entrypoint.sh"]
