#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/app"
DEPLOY_SCRIPT="${REPO_ROOT}/src/scripts/deploy.sh"
ETL_CONFIG="${REPO_ROOT}/src/ETL/Processor/config.json"
CHECK_INTERVAL_SECONDS="${OPENCNPJ_CHECK_INTERVAL_SECONDS:-3600}"

log() {
  printf '[docker-entrypoint] %s\n' "$*"
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Variável obrigatória não configurada: ${name}" >&2
    exit 1
  fi
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Comando obrigatório não encontrado: $1" >&2
    exit 1
  fi
}

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "Arquivo obrigatório não encontrado: $path" >&2
    exit 1
  fi
}

read_remote_name() {
  node -e '
    const fs = require("node:fs");
    const config = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
    const remoteBase = config?.Rclone?.RemoteBase;
    if (typeof remoteBase !== "string" || remoteBase.length === 0) {
      process.exit(1);
    }

    const separator = remoteBase.indexOf(":");
    if (separator <= 0) {
      process.exit(1);
    }

    process.stdout.write(remoteBase.slice(0, separator));
  ' "$ETL_CONFIG"
}

require_rclone_remote() {
  local remote_name="$1"
  local remote_label="${remote_name}:"
  local remotes

  if ! remotes="$(rclone listremotes 2>/dev/null)"; then
    echo "Não foi possível listar os remotes do rclone usando RCLONE_CONFIG=${RCLONE_CONFIG}." >&2
    exit 1
  fi

  if ! printf '%s\n' "$remotes" | grep -Fx "$remote_label" >/dev/null; then
    echo "Remote obrigatório do rclone não encontrado: ${remote_label}" >&2
    exit 1
  fi
}

if ! [[ "$CHECK_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || [[ "$CHECK_INTERVAL_SECONDS" -le 0 ]]; then
  echo "OPENCNPJ_CHECK_INTERVAL_SECONDS deve ser um inteiro positivo." >&2
  exit 1
fi

require_command rclone
require_command node
require_command dotnet
require_command npx
require_command base64

if [[ -z "${CLOUDFLARE_API_TOKEN:-}" && -n "${CF_API_TOKEN:-}" ]]; then
  export CLOUDFLARE_API_TOKEN="$CF_API_TOKEN"
fi

if [[ -z "${CLOUDFLARE_ACCOUNT_ID:-}" && -n "${CF_ACCOUNT_ID:-}" ]]; then
  export CLOUDFLARE_ACCOUNT_ID="$CF_ACCOUNT_ID"
fi

if [[ -z "${CLOUDFLARE_ZONE_ID:-}" && -n "${CF_ZONE_ID:-}" ]]; then
  export CLOUDFLARE_ZONE_ID="$CF_ZONE_ID"
fi

require_env CLOUDFLARE_API_TOKEN

TMP_RCLONE_CONFIG=""
cleanup() {
  if [[ -n "$TMP_RCLONE_CONFIG" && -f "$TMP_RCLONE_CONFIG" ]]; then
    rm -f "$TMP_RCLONE_CONFIG"
  fi
}
trap cleanup EXIT

if [[ -n "${RCLONE_CONFIG_BASE64:-}" ]]; then
  TMP_RCLONE_CONFIG="$(mktemp /tmp/rclone-config.XXXXXX.conf)"
  if ! printf '%s' "$RCLONE_CONFIG_BASE64" | base64 -d > "$TMP_RCLONE_CONFIG" 2>/dev/null; then
    echo "RCLONE_CONFIG_BASE64 inválido; não foi possível decodificar a configuração do rclone." >&2
    exit 1
  fi

  export RCLONE_CONFIG="$TMP_RCLONE_CONFIG"
elif [[ -n "${RCLONE_CONFIG:-}" ]]; then
  require_file "$RCLONE_CONFIG"
else
  echo "Configure RCLONE_CONFIG_BASE64 ou RCLONE_CONFIG antes de iniciar o container." >&2
  exit 1
fi

RCLONE_REMOTE_NAME="$(read_remote_name)"
require_rclone_remote "$RCLONE_REMOTE_NAME"

if [[ -n "${CLOUDFLARE_ACCOUNT_ID:-}" ]]; then
  log "CLOUDFLARE_ACCOUNT_ID configurado"
else
  log "CLOUDFLARE_ACCOUNT_ID não configurado; o wrangler precisa conseguir resolver a conta sem ele"
fi

log "rclone remoto '${RCLONE_REMOTE_NAME}:' validado"
log "Iniciando loop horário (${CHECK_INTERVAL_SECONDS}s)"

while true; do
  "${DEPLOY_SCRIPT}" --cleanup-on-success
  sleep "${CHECK_INTERVAL_SECONDS}"
done
