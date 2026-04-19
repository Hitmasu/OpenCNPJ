import { CACHE_TTL_SECONDS, JSON_HEADERS, NO_STORE_HEADERS } from "./constants.ts";

export async function handleCachedJson(
  cacheKeyUrl: string,
  ctx: ExecutionContext,
  loader: () => Promise<Response>,
): Promise<Response> {
  const cache = caches.default;
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  const cached = await cache.match(cacheKey);
  if (cached) {
    return cached;
  }

  const response = await loader();
  if (response.ok) {
    ctx.waitUntil(cache.put(cacheKey, response.clone()));
  }

  return response;
}

export function jsonOk(data: unknown): Response {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: {
      ...JSON_HEADERS,
      "Cache-Control": `public, max-age=${CACHE_TTL_SECONDS}, s-maxage=${CACHE_TTL_SECONDS}`,
    },
  });
}

export function jsonOkNoStore(data: unknown): Response {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: NO_STORE_HEADERS,
  });
}

export function jsonError(status: number, message: string): Response {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: NO_STORE_HEADERS,
  });
}

export function corsPreflight(): Response {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Accept",
      "Access-Control-Max-Age": String(CACHE_TTL_SECONDS),
    },
  });
}
