FROM mcr.microsoft.com/dotnet/sdk:10.0

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Sao_Paulo

ARG RCLONE_VERSION=1.75.0

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
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /etc/apt/keyrings/google-cloud.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_24.x nodistro main" \
        > /etc/apt/sources.list.d/nodesource.list \
    && echo "deb [signed-by=/etc/apt/keyrings/google-cloud.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) \
        rclone_arch="amd64"; \
        rclone_sha256="aa2804e08f48250e71009c727124b6341cd0288465804a9a09d14663cabafbaa" \
        ;; \
      arm64) \
        rclone_arch="arm64"; \
        rclone_sha256="d0ad88ba4c8e285b7c9efa591e0ab643280a91741e13c27f3a9c0957ccfa5203" \
        ;; \
      *) echo "Unsupported architecture for rclone: $arch" >&2; exit 1 ;; \
    esac; \
    archive="rclone-v${RCLONE_VERSION}-linux-${rclone_arch}.zip"; \
    curl -fsSL "https://downloads.rclone.org/v${RCLONE_VERSION}/${archive}" -o "/tmp/${archive}"; \
    echo "${rclone_sha256}  /tmp/${archive}" | sha256sum -c -; \
    unzip -q "/tmp/${archive}" -d /tmp; \
    install -m 0755 "/tmp/rclone-v${RCLONE_VERSION}-linux-${rclone_arch}/rclone" /usr/local/bin/rclone; \
    ln -sf /usr/local/bin/rclone /usr/bin/rclone; \
    rm -rf "/tmp/${archive}" "/tmp/rclone-v${RCLONE_VERSION}-linux-${rclone_arch}"; \
    rclone version

WORKDIR /app

COPY . .

RUN chmod +x /app/src/scripts/deploy.sh /app/src/scripts/docker-entrypoint.sh \
    && npm ci --prefix /app/src/Worker \
    && ln -sf /app/src/Worker/node_modules/.bin/wrangler /usr/local/bin/wrangler \
    && dotnet restore /app/src/ETL/Processor/CNPJExporter.csproj

ENTRYPOINT ["/app/src/scripts/docker-entrypoint.sh"]
