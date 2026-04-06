#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SRC_DIR}/.." && pwd)"
ETL_DIR="${SRC_DIR}/ETL/Processor"
WORKER_DIR="${SRC_DIR}/Worker"
ETL_CONFIG="${ETL_DIR}/config.json"
WORKER_ASSETS_DIR="${WORKER_DIR}/assets"
WORKER_ASSETS_FILES_DIR="${WORKER_ASSETS_DIR}/files"
WORKER_ASSETS_SHARDS_DIR="${WORKER_ASSETS_FILES_DIR}/shards"
DEFAULT_VALIDATE_CNPJ="60701190000104"

MONTH=""
RELEASE_ID="${OPENCNPJ_RELEASE_ID:-}"
BASE_URL="${OPENCNPJ_BASE_URL:-}"
VALIDATE_CNPJ="${OPENCNPJ_VALIDATE_CNPJ:-$DEFAULT_VALIDATE_CNPJ}"
CLEANUP_ON_SUCCESS="false"
SKIP_DELETE_OLD="false"

usage() {
  cat <<'EOF'
Uso:
  src/script/deploy.sh [--month YYYY-MM] [--release-id ID] [--base-url URL] [--validate-cnpj CNPJ] [--cleanup-on-success] [--skip-delete-old]

Fluxo:
  1. Gera um release id curto
  2. Roda o ETL com upload versionado no R2
  3. Copia info.json e *.index.bin para src/Worker/assets
  4. Executa npm test no Worker
  5. Faz npx wrangler deploy
  6. Valida /info, /{cnpj} e /{cnpj mascarado}
  7. Remove o release antigo do bucket se o deploy novo estiver saudável
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --month|-m)
      MONTH="${2:-}"
      shift 2
      ;;
    --release-id)
      RELEASE_ID="${2:-}"
      shift 2
      ;;
    --base-url)
      BASE_URL="${2:-}"
      shift 2
      ;;
    --validate-cnpj)
      VALIDATE_CNPJ="${2:-}"
      shift 2
      ;;
    --cleanup-on-success)
      CLEANUP_ON_SUCCESS="true"
      shift
      ;;
    --skip-delete-old)
      SKIP_DELETE_OLD="true"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Argumento desconhecido: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

log() {
  printf '[deploy] %s\n' "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Comando obrigatório não encontrado: $1" >&2
    exit 1
  fi
}

require_command dotnet
require_command node
require_command npx
require_command curl
require_command rclone
require_command shasum

generate_release_id() {
  local seed
  seed="$(date -u +%Y%m%dT%H%M%SZ)|${MONTH:-latest}|$$|$RANDOM"
  printf '%s' "$seed" | shasum -a 256 | cut -c1-16
}

resolve_dataset_key() {
  if [[ -n "$MONTH" ]]; then
    printf '%s\n' "$MONTH"
    return
  fi

  local latest=""
  local candidate
  for candidate in "${ETL_DIR}/cnpj_shards"/*; do
    [[ -d "$candidate" ]] || continue
    local name
    name="$(basename "$candidate")"
    if [[ -z "$latest" || "$name" > "$latest" ]]; then
      latest="$name"
    fi
  done
  if [[ -z "$latest" ]]; then
    echo "Não foi possível determinar o dataset gerado em ${ETL_DIR}/cnpj_shards" >&2
    exit 1
  fi

  printf '%s\n' "$latest"
}

read_config_value() {
  local expression="$1"
  node -e '
    const fs = require("fs");
    const config = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
    const expression = process.argv[2].split(".");
    let current = config;
    for (const part of expression) current = current?.[part];
    if (current == null) process.exit(2);
    process.stdout.write(String(current));
  ' "$ETL_CONFIG" "$expression"
}

json_field() {
  local payload="$1"
  local field="$2"
  node -e '
    const payload = JSON.parse(process.argv[1]);
    const field = process.argv[2];
    const value = payload?.[field];
    if (value == null) process.exit(2);
    process.stdout.write(String(value));
  ' "$payload" "$field"
}

fetch_json() {
  local url="$1"
  curl -fsS "$url"
}

mask_cnpj_for_path() {
  local raw="$1"
  node -e '
    const input = process.argv[1].trim().toUpperCase().replace(/[./-]/g, "");
    if (input.length !== 14) process.exit(2);
    process.stdout.write(`${input.slice(0,2)}.${input.slice(2,5)}.${input.slice(5,8)}/${input.slice(8,14)}`);
  ' "$raw"
}

copy_worker_assets() {
  local dataset_key="$1"
  local release_id="$2"
  local source_root="${ETL_DIR}/cnpj_shards/${dataset_key}/releases/${release_id}"
  local source_shards="${source_root}/shards"
  local source_info="${source_root}/info.json"

  [[ -f "$source_info" ]] || { echo "info.json não encontrado em ${source_info}" >&2; exit 1; }
  [[ -d "$source_shards" ]] || { echo "Diretório de shards não encontrado em ${source_shards}" >&2; exit 1; }

  rm -rf "${WORKER_ASSETS_DIR}"
  mkdir -p "${WORKER_ASSETS_SHARDS_DIR}"
  cp "$source_info" "${WORKER_ASSETS_FILES_DIR}/info.json"
  find "$source_shards" -maxdepth 1 -type f -name '*.index.bin' -exec cp {} "${WORKER_ASSETS_SHARDS_DIR}/" \;
}

cleanup_worker_shard_assets() {
  rm -rf "${WORKER_ASSETS_SHARDS_DIR}"
}

validate_endpoint() {
  local url="$1"
  local expected_release="$2"

  local response
  response="$(fetch_json "$url")"

  printf '%s' "$response" | node -e '
    let raw = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", chunk => raw += chunk);
    process.stdin.on("end", () => {
      const payload = JSON.parse(raw);
      const expectedRelease = process.argv[1];
      const url = process.argv[2];
      if (payload.error) {
        console.error(`Endpoint retornou erro em ${url}: ${payload.error}`);
        process.exit(1);
      }

      if (url.endsWith("/info")) {
        if (payload.storage_release_id !== expectedRelease) {
          console.error(`storage_release_id inesperado em /info: ${payload.storage_release_id} != ${expectedRelease}`);
          process.exit(1);
        }
        process.exit(0);
      }

      if (!payload.cnpj) {
        console.error(`Payload inválido em ${url}: campo cnpj ausente`);
        process.exit(1);
      }
    });
  ' "$expected_release" "$url"
}

deploy_worker() {
  local deploy_output
  pushd "$WORKER_DIR" >/dev/null
  deploy_output="$(npx wrangler deploy 2>&1)"
  popd >/dev/null

  printf '%s\n' "$deploy_output" >&2

  if [[ -n "$BASE_URL" ]]; then
    printf '%s\n' "$BASE_URL"
    return
  fi

  local parsed_url
  parsed_url="$(printf '%s\n' "$deploy_output" | rg -o 'https://[A-Za-z0-9.-]+\.workers\.dev' | tail -n 1 || true)"
  if [[ -z "$parsed_url" ]]; then
    echo "Não foi possível identificar a URL publicada pelo wrangler deploy. Informe --base-url." >&2
    exit 1
  fi

  printf '%s\n' "$parsed_url"
}

delete_old_release() {
  local old_release_id="$1"
  [[ -n "$old_release_id" ]] || return 0
  [[ "$SKIP_DELETE_OLD" == "true" ]] && return 0

  local remote_base
  remote_base="$(read_config_value "Rclone.RemoteBase")"
  local old_remote="${remote_base%/}/shards/releases/${old_release_id}"

  log "Removendo release antigo ${old_release_id} em ${old_remote}"
  rclone purge "$old_remote"
}

capture_current_release() {
  if [[ -z "$BASE_URL" ]]; then
    return 0
  fi

  local current_info
  if ! current_info="$(fetch_json "${BASE_URL%/}/info" 2>/dev/null)"; then
    return 0
  fi

  json_field "$current_info" "storage_release_id" 2>/dev/null || true
}

if [[ -z "$RELEASE_ID" ]]; then
  RELEASE_ID="$(generate_release_id)"
fi
OLD_RELEASE_ID="$(capture_current_release || true)"

log "Release id novo: ${RELEASE_ID}"
if [[ -n "$OLD_RELEASE_ID" ]]; then
  log "Release atual em produção: ${OLD_RELEASE_ID}"
fi

PIPELINE_ARGS=(pipeline --release-id "$RELEASE_ID")
if [[ -n "$MONTH" ]]; then
  PIPELINE_ARGS+=(--month "$MONTH")
fi
if [[ "$CLEANUP_ON_SUCCESS" == "true" ]]; then
  PIPELINE_ARGS+=(--cleanup-on-success)
fi

log "Executando ETL"
pushd "$ETL_DIR" >/dev/null
set +e
dotnet run "${PIPELINE_ARGS[@]}"
PIPELINE_EXIT_CODE=$?
set -e
popd >/dev/null

if [[ "$PIPELINE_EXIT_CODE" -eq 10 ]]; then
  log "Nenhuma base nova para publicar; deploy encerrado sem alterações."
  exit 0
fi

if [[ "$PIPELINE_EXIT_CODE" -ne 0 ]]; then
  exit "$PIPELINE_EXIT_CODE"
fi

DATASET_KEY="$(resolve_dataset_key)"
log "Dataset usado: ${DATASET_KEY}"

log "Copiando assets do Worker"
copy_worker_assets "$DATASET_KEY" "$RELEASE_ID"

log "Rodando testes do Worker"
pushd "$WORKER_DIR" >/dev/null
npm test
popd >/dev/null

log "Fazendo deploy do Worker"
DEPLOY_URL="$(deploy_worker | tail -n 1)"
log "URL de validação: ${DEPLOY_URL}"

MASKED_VALIDATE_CNPJ="$(mask_cnpj_for_path "$VALIDATE_CNPJ")"

log "Validando /info"
validate_endpoint "${DEPLOY_URL%/}/info" "$RELEASE_ID"

log "Validando /${VALIDATE_CNPJ}"
validate_endpoint "${DEPLOY_URL%/}/${VALIDATE_CNPJ}" "$RELEASE_ID"

log "Validando /${MASKED_VALIDATE_CNPJ}"
validate_endpoint "${DEPLOY_URL%/}/${MASKED_VALIDATE_CNPJ}" "$RELEASE_ID"

if [[ -n "$OLD_RELEASE_ID" && "$OLD_RELEASE_ID" != "$RELEASE_ID" ]]; then
  delete_old_release "$OLD_RELEASE_ID"
fi

log "Limpando shards staged do frontend"
cleanup_worker_shard_assets

log "Deploy concluído com sucesso"
