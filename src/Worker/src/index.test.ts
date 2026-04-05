import assert from "node:assert/strict";
import test from "node:test";

import worker, { type Env, __test__ } from "./index.ts";

type StoredObject = {
  text(): Promise<string>;
};

class FakeCache {
  private readonly store = new Map<string, Response>();

  async match(request: Request): Promise<Response | undefined> {
    const cached = this.store.get(request.url);
    return cached?.clone();
  }

  async put(request: Request, response: Response): Promise<void> {
    this.store.set(request.url, response.clone());
  }
}

class FakeBucket {
  public readonly gets: Array<{ key: string; range?: { offset: number; length: number } }> = [];
  private readonly objects: Record<string, string>;

  public constructor(objects: Record<string, string>) {
    this.objects = objects;
  }

  async get(key: string, options?: { range?: { offset: number; length: number } }): Promise<StoredObject | null> {
    this.gets.push({ key, range: options?.range });
    const content = this.objects[key];
    if (content == null) {
      return null;
    }

    if (!options?.range) {
      return {
        async text() {
          return content;
        },
      };
    }

    const chunk = content.slice(options.range.offset, options.range.offset + options.range.length);
    return {
      async text() {
        return chunk;
      },
    };
  }
}

class FakeAssetsFetcher {
  public readonly requests: string[] = [];
  private readonly assets: Record<string, string | Uint8Array>;

  public constructor(assets: Record<string, string | Uint8Array>) {
    this.assets = assets;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    this.requests.push(url.pathname);
    const asset = this.assets[url.pathname];
    if (asset == null) {
      return new Response("not found", { status: 404 });
    }

    const body = typeof asset === "string" ? asset : asset.slice();
    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type": typeof asset === "string"
          ? "application/json; charset=utf-8"
          : "application/octet-stream",
      },
    });
  }
}

function installFakeCache(): FakeCache {
  const cache = new FakeCache();
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
  return cache;
}

function createExecutionContext(): ExecutionContext {
  const pending: Promise<unknown>[] = [];

  return {
    waitUntil(promise: Promise<unknown>) {
      pending.push(promise);
    },
    passThroughOnException() {},
    props: {},
  } as ExecutionContext;
}

function createLookupFixture() {
  const cnpj = "00000000000000";
  const payload = { razao_social: "EMPRESA TESTE LTDA" };
  const line = `${JSON.stringify({ cnpj, data: payload })}\n`;

  return {
    cnpj,
    payload,
    ndjson: line,
    index: buildBinaryIndex([
      { cnpj, offset: 0, length: line.length },
    ]),
  };
}

function buildBinaryIndex(entries: Array<{ cnpj: string; offset: number; length: number }>): Uint8Array {
  const headerSize = 8;
  const entrySize = 26;
  const cnpjLength = 14;
  const index = new Uint8Array(headerSize + (entries.length * entrySize));
  index.set([0x4f, 0x43, 0x49, 0x31], 0);

  const view = new DataView(index.buffer);
  view.setUint32(4, entries.length, true);

  entries.forEach((entry, entryIndex) => {
    const start = headerSize + (entryIndex * entrySize);
    const cnpjBytes = new TextEncoder().encode(entry.cnpj);
    index.set(cnpjBytes, start);
    view.setUint32(start + cnpjLength, entry.offset >>> 0, true);
    view.setUint32(start + cnpjLength + 4, Math.floor(entry.offset / 0x1_0000_0000), true);
    view.setUint32(start + cnpjLength + 8, entry.length, true);
  });

  return index;
}

test.beforeEach(() => {
  __test__.clearHotIndexCache();
  installFakeCache();
});

test("normalizeCnpj accepts numeric formats with and without mask", () => {
  assert.equal(__test__.normalizeCnpj("00000000000000"), "00000000000000");
  assert.equal(__test__.normalizeCnpj("00.000.000/0000-00"), "00000000000000");
  assert.equal(__test__.normalizeCnpj("00.000.000/000000"), "00000000000000");
});

test("normalizeCnpj accepts alphanumeric CNPJ with mask", () => {
  assert.equal(__test__.normalizeCnpj("AB.CDE.FGH/IJKL-12"), "ABCDEFGHIJKL12");
});

test("extractCnpjFromPath accepts slash in masked path", () => {
  assert.equal(__test__.extractCnpjFromPath("/00.000.000/0000-00"), "00000000000000");
  assert.equal(__test__.extractCnpjFromPath("/00.000.000/000000"), "00000000000000");
  assert.equal(__test__.extractCnpjFromPath("/AB.CDE.FGH/IJKL-12"), "ABCDEFGHIJKL12");
});

test("extractCnpjFromPath rejects unrelated multi-segment paths", () => {
  assert.equal(__test__.extractCnpjFromPath("/foo/bar/baz"), null);
});

test("fetch returns a record using binary asset index and exact R2 range read", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
    "/files/info.json": JSON.stringify({
      storage_release_id: "release-123",
      shard_path_template: "shards/releases/{release_id}/{prefix}.ndjson",
    }),
    "/files/shards/000.index.bin": fixture.index,
  });

  const response = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}`),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), fixture.payload);
  assert.deepEqual(
    [...assets.requests].sort(),
    ["/files/info.json", "/files/shards/000.index.bin"].sort(),
  );
  assert.deepEqual(bucket.gets, [
    {
      key: "files/shards/releases/release-123/000.ndjson",
      range: { offset: 0, length: fixture.ndjson.length },
    },
  ]);
});

test("fetch canonicalizes masked URLs in cache", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
    "/files/info.json": JSON.stringify({
      storage_release_id: "release-123",
      shard_path_template: "shards/releases/{release_id}/{prefix}.ndjson",
    }),
    "/files/shards/000.index.bin": fixture.index,
  });
  const env = {
    CNPJ_BUCKET: bucket as unknown as R2Bucket,
    ASSETS: assets as unknown as Fetcher,
  } satisfies Env;

  const first = await worker.fetch(
    new Request("https://worker.invalid/00.000.000/0000-00"),
    env,
    createExecutionContext(),
  );
  const second = await worker.fetch(
    new Request("https://worker.invalid/00000000000000"),
    env,
    createExecutionContext(),
  );

  assert.equal(first.status, 200);
  assert.equal(second.status, 200);
  assert.equal(assets.requests.length, 2);
  assert.equal(bucket.gets.length, 1);
});

test("fetch returns 404 when the binary index asset is missing", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
    "/files/info.json": JSON.stringify({
      storage_release_id: "release-123",
      shard_path_template: "shards/releases/{release_id}/{prefix}.ndjson",
    }),
  });

  const response = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}`),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 404);
  assert.deepEqual(await response.json(), { error: "not found" });
  assert.equal(bucket.gets.length, 0);
});

test("fetch returns 404 when the record is not present in the binary index", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
    "/files/info.json": JSON.stringify({
      storage_release_id: "release-123",
      shard_path_template: "shards/releases/{release_id}/{prefix}.ndjson",
    }),
    "/files/shards/000.index.bin": buildBinaryIndex([
      { cnpj: "00000000000001", offset: 0, length: fixture.ndjson.length },
    ]),
  });

  const response = await worker.fetch(
    new Request("https://worker.invalid/00000000000002"),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 404);
  assert.deepEqual(await response.json(), { error: "not found" });
  assert.equal(bucket.gets.length, 0);
});

test("fetch serves /info from static assets", async () => {
  const bucket = new FakeBucket({});
  const assets = new FakeAssetsFetcher({
    "/files/info.json": JSON.stringify({ versao: "2026-03" }),
  });

  const response = await worker.fetch(
    new Request("https://worker.invalid/info"),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { versao: "2026-03" });
  assert.equal(bucket.gets.length, 0);
});

test("fetch reuses hot chunk cache when response cache is cold but range is already hot", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
    "/files/info.json": JSON.stringify({
      storage_release_id: "release-123",
      shard_path_template: "shards/releases/{release_id}/{prefix}.ndjson",
    }),
    "/files/shards/000.index.bin": fixture.index,
  });
  const env = {
    CNPJ_BUCKET: bucket as unknown as R2Bucket,
    ASSETS: assets as unknown as Fetcher,
  } satisfies Env;

  const first = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}`),
    env,
    createExecutionContext(),
  );

  assert.equal(first.status, 200);

  installFakeCache();

  const second = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}`),
    env,
    createExecutionContext(),
  );

  assert.equal(second.status, 200);
  assert.deepEqual(await second.json(), fixture.payload);
  assert.equal(assets.requests.length, 2);
  assert.deepEqual(bucket.gets, [
    { key: "files/shards/releases/release-123/000.ndjson", range: { offset: 0, length: fixture.ndjson.length } },
  ]);
});
