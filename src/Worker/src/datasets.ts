import type { DatasetSelection, RuntimeInfo } from "./types.ts";

const RECEITA_DATASET_KEY = "receita";
const IGNORED_DATASET_TOKENS = new Set(["e", "and"]);

export type DatasetSelectionResult =
  | { ok: true; value: DatasetSelection }
  | { ok: false; error: string };

export function resolveDatasetSelection(searchParams: URLSearchParams, runtimeInfo: RuntimeInfo | null): DatasetSelectionResult {
  const availableModuleKeys = Object
    .keys(runtimeInfo?.datasets ?? {})
    .filter(key => key !== RECEITA_DATASET_KEY)
    .sort((left, right) => left.localeCompare(right));
  const requestedKeys = parseRequestedDatasetKeys(searchParams);

  if (requestedKeys.length === 0) {
    return {
      ok: true,
      value: {
        includeReceita: true,
        moduleKeys: availableModuleKeys,
        cacheKey: [RECEITA_DATASET_KEY, ...availableModuleKeys].join(","),
      },
    };
  }

  const available = new Set([RECEITA_DATASET_KEY, ...availableModuleKeys]);
  const unknown = requestedKeys.filter(key => !available.has(key));
  if (unknown.length > 0) {
    return {
      ok: false,
      error: `invalid dataset: ${unknown.join(",")}`,
    };
  }

  const includeReceita = requestedKeys.includes(RECEITA_DATASET_KEY);
  const moduleKeys = requestedKeys
    .filter(key => key !== RECEITA_DATASET_KEY)
    .sort((left, right) => left.localeCompare(right));

  return {
    ok: true,
    value: {
      includeReceita,
      moduleKeys,
      cacheKey: requestedKeys.join(","),
    },
  };
}

function parseRequestedDatasetKeys(searchParams: URLSearchParams): string[] {
  const values = [
    ...searchParams.getAll("datasets"),
    ...searchParams.getAll("dataset"),
  ];
  const keys = new Set<string>();

  for (const value of values) {
    for (const token of value.split(/[,\s]+/)) {
      const normalized = token.trim().toLowerCase();
      if (!normalized || IGNORED_DATASET_TOKENS.has(normalized)) {
        continue;
      }

      keys.add(normalized);
    }
  }

  return [...keys];
}
