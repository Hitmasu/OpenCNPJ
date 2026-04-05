import { ALPHANUMERIC_CNPJ_PATTERN, CNPJ_MASK_CHARACTERS } from "./constants.ts";

export function extractCnpjFromPath(pathname: string): string | null {
  const decodedPath = safeDecodeURIComponent(pathname).trim();
  const candidate = decodedPath.replace(/^\/+|\/+$/g, "");
  if (!candidate) {
    return null;
  }

  const slashCount = candidate.split("/").length - 1;
  if (slashCount > 1) {
    return null;
  }

  return normalizeCnpj(candidate);
}

export function normalizeCnpj(value: string): string | null {
  const normalized = value.trim().toUpperCase().replace(CNPJ_MASK_CHARACTERS, "");
  return ALPHANUMERIC_CNPJ_PATTERN.test(normalized) ? normalized : null;
}

function safeDecodeURIComponent(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

