export const DEFAULT_ROUTE = '/sobre';

export function normalizeRoute(hash: string): string {
  const raw = hash.replace(/^#/, '').trim();
  if (!raw || raw === '/') return DEFAULT_ROUTE;

  const withLeadingSlash = raw.startsWith('/') ? raw : `/${raw}`;
  return withLeadingSlash.replace(/\/+$/, '') || DEFAULT_ROUTE;
}

export function toHash(route: string): `#/${string}` {
  const normalized = route.startsWith('/') ? route.slice(1) : route;
  return `#/${normalized}`;
}
