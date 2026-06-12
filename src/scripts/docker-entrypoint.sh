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

read_config_value() {
  local expression="$1"
  node -e '
    const fs = require("node:fs");
    const config = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
    const expression = process.argv[2].split(".");
    let current = config;
    for (const part of expression) current = current?.[part];
    if (current == null) process.exit(2);
    process.stdout.write(String(current));
  ' "$ETL_CONFIG" "$expression"
}

read_bigquery_enabled() {
  if [[ -n "${OPENCNPJ_BIGQUERY_ENABLED:-}" ]]; then
    case "${OPENCNPJ_BIGQUERY_ENABLED,,}" in
      true)
        printf 'true\n'
        return 0
        ;;
      false)
        printf 'false\n'
        return 0
        ;;
      *)
        echo "OPENCNPJ_BIGQUERY_ENABLED deve ser true ou false." >&2
        exit 1
        ;;
    esac
  fi

  read_config_value "BigQuery.Enabled"
}

read_bigquery_project_id() {
  if [[ -n "${OPENCNPJ_BIGQUERY_PROJECT_ID:-}" ]]; then
    printf '%s\n' "$OPENCNPJ_BIGQUERY_PROJECT_ID"
    return 0
  fi

  read_config_value "BigQuery.ProjectId"
}

read_remote_name() {
  node -e '
    const remoteBase = process.argv[1];
    if (typeof remoteBase !== "string" || remoteBase.length === 0) {
      process.exit(1);
    }

    const separator = remoteBase.indexOf(":");
    if (separator <= 0) {
      process.exit(1);
    }

    process.stdout.write(remoteBase.slice(0, separator));
  ' "$(read_config_value "Rclone.RemoteBase")"
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

activate_bigquery_credentials_if_configured() {
  local enabled
  enabled="$(read_bigquery_enabled 2>/dev/null || printf 'false')"
  if [[ "$enabled" != "true" ]]; then
    return 0
  fi

  if [[ -z "${OPENCNPJ_GOOGLE_CREDENTIALS_BASE64:-}" ]]; then
    log "OPENCNPJ_GOOGLE_CREDENTIALS_BASE64 não configurado; usando credenciais já disponíveis para o bq."
    return 0
  fi

  local project_id
  project_id="$(read_bigquery_project_id 2>/dev/null || true)"
  if [[ -z "$project_id" ]]; then
    echo "BigQuery.ProjectId ou OPENCNPJ_BIGQUERY_PROJECT_ID é obrigatório quando BigQuery.Enabled=true." >&2
    exit 1
  fi

  require_command gcloud
  TMP_GOOGLE_CREDENTIALS="$(mktemp /tmp/google-credentials.XXXXXX.json)"
  if ! printf '%s' "$OPENCNPJ_GOOGLE_CREDENTIALS_BASE64" | base64 -d > "$TMP_GOOGLE_CREDENTIALS" 2>/dev/null; then
    echo "OPENCNPJ_GOOGLE_CREDENTIALS_BASE64 inválido; não foi possível decodificar a credencial Google." >&2
    exit 1
  fi

  chmod 600 "$TMP_GOOGLE_CREDENTIALS"
  export CLOUDSDK_CORE_DISABLE_PROMPTS="${CLOUDSDK_CORE_DISABLE_PROMPTS:-1}"
  gcloud auth activate-service-account \
    --key-file="$TMP_GOOGLE_CREDENTIALS" \
    --project="$project_id" \
    --quiet >/dev/null
  rm -f "$TMP_GOOGLE_CREDENTIALS"
  TMP_GOOGLE_CREDENTIALS=""
  log "Credenciais BigQuery ativadas via OPENCNPJ_GOOGLE_CREDENTIALS_BASE64"
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
TMP_GOOGLE_CREDENTIALS=""
cleanup() {
  if [[ -n "$TMP_RCLONE_CONFIG" && -f "$TMP_RCLONE_CONFIG" ]]; then
    rm -f "$TMP_RCLONE_CONFIG"
  fi
  if [[ -n "$TMP_GOOGLE_CREDENTIALS" && -f "$TMP_GOOGLE_CREDENTIALS" ]]; then
    rm -f "$TMP_GOOGLE_CREDENTIALS"
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
activate_bigquery_credentials_if_configured

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
