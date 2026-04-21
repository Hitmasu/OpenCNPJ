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
WORKER_GENERATED_RUNTIME_INFO="${WORKER_DIR}/src/generated-runtime-info.ts"
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

  local release_info
  while IFS= read -r release_info; do
    local dataset_dir
    dataset_dir="$(basename "$(dirname "$(dirname "$release_info")")")"
    if [[ "$dataset_dir" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
      printf '%s\n' "$dataset_dir"
      return
    fi
  done < <(find "${ETL_DIR}/cnpj_shards" -path "*/releases/${RELEASE_ID}/info.json" -type f 2>/dev/null | sort)

  local latest=""
  local candidate
  for candidate in "${ETL_DIR}/cnpj_shards"/*; do
    [[ -d "$candidate" ]] || continue
    local name
    name="$(basename "$candidate")"
    [[ "$name" =~ ^[0-9]{4}-[0-9]{2}$ ]] || continue
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
  local max_attempts="${OPENCNPJ_FETCH_JSON_RETRIES:-6}"
  local retry_delay_seconds="${OPENCNPJ_FETCH_JSON_RETRY_DELAY_SECONDS:-5}"
  local attempt=1
  local curl_exit_code=0

  while true; do
    if curl -fsS "$url"; then
      return 0
    fi

    curl_exit_code=$?
    if [[ "$attempt" -ge "$max_attempts" ]]; then
      return "$curl_exit_code"
    fi

    log "Falha ao buscar ${url} (tentativa ${attempt}/${max_attempts}); tentando novamente em ${retry_delay_seconds}s" >&2
    sleep "$retry_delay_seconds"
    attempt=$((attempt + 1))
  done
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
  local module_assets_root="${ETL_DIR}/cnpj_shards/shards/modules"

  [[ -f "$source_info" ]] || { echo "info.json não encontrado em ${source_info}" >&2; exit 1; }

  rm -rf "${WORKER_ASSETS_DIR}"
  mkdir -p "${WORKER_ASSETS_SHARDS_DIR}"
  cp "$source_info" "${WORKER_ASSETS_FILES_DIR}/info.json"
  if [[ -d "$source_shards" ]]; then
    find "$source_shards" -maxdepth 1 -type f -name '*.index.bin' -exec cp {} "${WORKER_ASSETS_SHARDS_DIR}/" \;
  fi

  if [[ -d "$module_assets_root" ]]; then
    while IFS= read -r module_index; do
      local relative_path
      local destination
      relative_path="${module_index#"${ETL_DIR}/cnpj_shards/"}"
      relative_path="${relative_path/\/releases\//\/}"
      destination="${WORKER_ASSETS_FILES_DIR}/${relative_path}"
      mkdir -p "$(dirname "$destination")"
      cp "$module_index" "$destination"
    done < <(find "$module_assets_root" -path "*/releases/${release_id}/*.index.bin" -type f)
  fi

}

generate_worker_runtime_info() {
  local dataset_key="$1"
  local release_id="$2"
  local source_info="${ETL_DIR}/cnpj_shards/${dataset_key}/releases/${release_id}/info.json"

  [[ -f "$source_info" ]] || { echo "info.json não encontrado em ${source_info}" >&2; exit 1; }

  node -e '
    const fs = require("fs");
    const source = process.argv[1];
    const destination = process.argv[2];
    const payload = JSON.parse(fs.readFileSync(source, "utf8"));
    const body = `${JSON.stringify(payload, null, 2)} as RuntimeInfo`;
    fs.writeFileSync(destination, [
      "import type { RuntimeInfo } from \"./types.ts\";",
      "",
      `export const GENERATED_RUNTIME_INFO = ${body};`,
      "",
      "let runtimeInfoForTest: RuntimeInfo | null | undefined;",
      "",
      "export function getEmbeddedRuntimeInfo(): RuntimeInfo | null {",
      "  if (runtimeInfoForTest !== undefined) {",
      "    return runtimeInfoForTest;",
      "  }",
      "",
      "  return GENERATED_RUNTIME_INFO;",
      "}",
      "",
      "export function hasEmbeddedRuntimeInfo(): boolean {",
      "  return getEmbeddedRuntimeInfo() != null;",
      "}",
      "",
      "export function setEmbeddedRuntimeInfoForTest(value: RuntimeInfo | null | undefined): void {",
      "  runtimeInfoForTest = value;",
      "}",
      "",
    ].join("\n"));
  ' "$source_info" "$WORKER_GENERATED_RUNTIME_INFO"
}

cleanup_worker_shard_assets() {
  rm -rf "${WORKER_ASSETS_SHARDS_DIR}"
}

cleanup_dataset_artifacts() {
  local dataset_key="$1"
  if [[ ! "$dataset_key" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
    echo "Dataset inválido para cleanup: ${dataset_key}" >&2
    exit 1
  fi

  local path_names=("DownloadDir" "DataDir")
  local path_name
  local data_dir=""
  for path_name in "${path_names[@]}"; do
    local configured_path
    configured_path="$(read_config_value "Paths.${path_name}")"
    local dataset_path
    dataset_path="$(node -e '
      const path = require("path");
      process.stdout.write(path.resolve(process.argv[1], process.argv[2], process.argv[3]));
    ' "$ETL_DIR" "$configured_path" "$dataset_key")"

    rm -rf "$dataset_path"

    if [[ "$path_name" == "DataDir" ]]; then
      data_dir="$(node -e '
        const path = require("path");
        process.stdout.write(path.resolve(process.argv[1], process.argv[2]));
      ' "$ETL_DIR" "$configured_path")"
    fi
  done

  if [[ -n "$data_dir" && -d "${data_dir}/integrations" ]]; then
    find "${data_dir}/integrations" -mindepth 1 -maxdepth 1 ! -name '_state' -exec rm -rf {} +
    find "${data_dir}/integrations" -mindepth 1 -maxdepth 1 -type f -delete
  fi

  local duckdb_in_memory
  duckdb_in_memory="$(read_config_value "DuckDb.UseInMemory" 2>/dev/null || printf 'false')"
  if [[ "$duckdb_in_memory" != "true" ]]; then
    rm -f "${ETL_DIR}/cnpj.duckdb"
  fi

  rm -rf "${ETL_DIR}/temp"
}

validate_endpoint() {
  local url="$1"
  local expected_release="$2"
  local retry_count="${OPENCNPJ_VALIDATE_RETRIES:-12}"
  local retry_delay_seconds="${OPENCNPJ_VALIDATE_RETRY_DELAY_SECONDS:-5}"
  local attempt=1

  while true; do
    local response
    response="$(fetch_json "$url")"

    if printf '%s' "$response" | node -e '
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
        function collectBaseReleases(info) {
          const releases = new Set();
          if (info?.storage_release_id) releases.add(info.storage_release_id);
          return releases;
        }

        function collectModuleReleases(info) {
          const releases = new Set();
          for (const [key, datasetInfo] of Object.entries(info?.datasets ?? {})) {
            if (key === "receita") continue;
            if (datasetInfo?.storage_release_id) releases.add(datasetInfo.storage_release_id);
          }
          return releases;
        }

        const releaseMatches =
          collectBaseReleases(payload).has(expectedRelease) ||
          collectModuleReleases(payload).has(expectedRelease);

        if (!releaseMatches) {
          console.error(`release ${expectedRelease} não está referenciado em /info`);
          process.exit(1);
        }
        process.exit(0);
      }

      if (!payload.cnpj) {
        console.error(`Payload inválido em ${url}: campo cnpj ausente`);
        process.exit(1);
      }
    });
  ' "$expected_release" "$url"; then
      return 0
    fi

    if [[ "$attempt" -ge "$retry_count" ]]; then
      return 1
    fi

    sleep "$retry_delay_seconds"
    attempt=$((attempt + 1))
  done
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
  parsed_url="$(printf '%s\n' "$deploy_output" | grep -Eo 'https://[A-Za-z0-9.-]+\.workers\.dev' | tail -n 1 || true)"
  if [[ -z "$parsed_url" ]]; then
    echo "Não foi possível identificar a URL publicada pelo wrangler deploy. Informe --base-url." >&2
    exit 1
  fi

  printf '%s\n' "$parsed_url"
}

purge_cloudflare_cache() {
  if [[ -z "${CLOUDFLARE_ZONE_ID:-}" ]]; then
    log "CLOUDFLARE_ZONE_ID não configurado; pulando purge do cache da zona"
    return 0
  fi

  if [[ -z "${CLOUDFLARE_API_TOKEN:-}" ]]; then
    log "CLOUDFLARE_API_TOKEN não configurado; pulando purge do cache da zona"
    return 0
  fi

  log "Executando purge do cache da Cloudflare"
  local response
  response="$(curl -fsS \
    -X POST "https://api.cloudflare.com/client/v4/zones/${CLOUDFLARE_ZONE_ID}/purge_cache" \
    -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
    -H "Content-Type: application/json" \
    --data '{"purge_everything":true}')"

  printf '%s' "$response" | node -e '
    let raw = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", chunk => raw += chunk);
    process.stdin.on("end", () => {
      const payload = JSON.parse(raw);
      if (!payload.success) {
        console.error(`Cloudflare purge falhou: ${JSON.stringify(payload.errors ?? payload)}`);
        process.exit(1);
      }
    });
  '
}

delete_old_releases() {
  local old_info="$1"
  local new_info="$2"
  [[ "$SKIP_DELETE_OLD" == "true" ]] && return 0
  [[ -n "$old_info" && -n "$new_info" ]] || return 0

  local remote_base
  remote_base="$(read_config_value "Rclone.RemoteBase")"

  node -e '
    const oldInfo = JSON.parse(process.argv[1]);
    const newInfo = JSON.parse(process.argv[2]);

    function collectBaseReleases(info) {
      const releases = new Set();
      if (info?.storage_release_id) releases.add(info.storage_release_id);
      return releases;
    }

    function collectModuleReleases(info) {
      const releasesByModule = new Map();
      for (const [key, moduleInfo] of Object.entries(info?.datasets ?? {})) {
        if (key === "receita") continue;
        const releases = releasesByModule.get(key) ?? new Set();
        if (moduleInfo?.storage_release_id) releases.add(moduleInfo.storage_release_id);
        releasesByModule.set(key, releases);
      }
      return releasesByModule;
    }

    const newBase = collectBaseReleases(newInfo);
    for (const release of collectBaseReleases(oldInfo)) {
      if (!newBase.has(release)) console.log(`base\t${release}`);
    }

    const oldModules = collectModuleReleases(oldInfo);
    const newModules = collectModuleReleases(newInfo);
    for (const [key, oldReleases] of oldModules.entries()) {
      const newReleases = newModules.get(key) ?? new Set();
      for (const release of oldReleases) {
        if (!newReleases.has(release)) console.log(`module\t${key}\t${release}`);
      }
    }
  ' "$old_info" "$new_info" | while IFS=$'\t' read -r kind first second; do
    if [[ "$kind" == "base" && -n "$first" ]]; then
      local old_remote="${remote_base%/}/shards/releases/${first}"
      log "Removendo release base antigo ${first} em ${old_remote}"
      rclone purge "$old_remote"
    elif [[ "$kind" == "module" && -n "$first" && -n "$second" ]]; then
      local old_remote="${remote_base%/}/shards/modules/${first}/${second}"
      log "Removendo release antigo do módulo ${first}/${second} em ${old_remote}"
      rclone purge "$old_remote"
    fi
  done
}

capture_current_info() {
  if [[ -z "$BASE_URL" ]]; then
    return 0
  fi

  if ! fetch_json "${BASE_URL%/}/info" 2>/dev/null; then
    return 0
  fi
}

if [[ -z "$RELEASE_ID" ]]; then
  RELEASE_ID="$(generate_release_id)"
fi
OLD_INFO="$(capture_current_info || true)"
OLD_RELEASE_ID=""
if [[ -n "$OLD_INFO" ]]; then
  OLD_RELEASE_ID="$(json_field "$OLD_INFO" "storage_release_id" 2>/dev/null || true)"
fi

log "Release id novo: ${RELEASE_ID}"
if [[ -n "$OLD_RELEASE_ID" ]]; then
  log "Release atual em produção: ${OLD_RELEASE_ID}"
fi

PIPELINE_ARGS=(pipeline --release-id "$RELEASE_ID")
if [[ -n "$MONTH" ]]; then
  PIPELINE_ARGS+=(--month "$MONTH")
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
generate_worker_runtime_info "$DATASET_KEY" "$RELEASE_ID"

log "Rodando testes do Worker"
pushd "$WORKER_DIR" >/dev/null
npm test
popd >/dev/null

log "Fazendo deploy do Worker"
DEPLOY_URL="$(deploy_worker | tail -n 1)"
log "URL de validação: ${DEPLOY_URL}"

purge_cloudflare_cache

MASKED_VALIDATE_CNPJ="$(mask_cnpj_for_path "$VALIDATE_CNPJ")"

log "Validando /info"
validate_endpoint "${DEPLOY_URL%/}/info" "$RELEASE_ID"

log "Validando /${VALIDATE_CNPJ}"
validate_endpoint "${DEPLOY_URL%/}/${VALIDATE_CNPJ}" "$RELEASE_ID"

log "Validando /${MASKED_VALIDATE_CNPJ}"
validate_endpoint "${DEPLOY_URL%/}/${MASKED_VALIDATE_CNPJ}" "$RELEASE_ID"

NEW_INFO="$(fetch_json "${DEPLOY_URL%/}/info")"
delete_old_releases "$OLD_INFO" "$NEW_INFO"

if [[ "$CLEANUP_ON_SUCCESS" == "true" ]]; then
  log "Limpando artefatos locais de ${DATASET_KEY}"
  cleanup_dataset_artifacts "$DATASET_KEY"
fi

log "Limpando shards staged do frontend"
cleanup_worker_shard_assets

log "Deploy concluído com sucesso"
