import assert from "node:assert/strict";
import test from "node:test";

import worker, { type Env, __test__ } from "./index.ts";

type StoredObject = {
  text(): Promise<string>;
  arrayBuffer(): Promise<ArrayBuffer>;
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
  private readonly objects: Record<string, string | Uint8Array>;

  public constructor(objects: Record<string, string | Uint8Array>) {
    this.objects = objects;
  }

  async get(key: string, options?: { range?: { offset: number; length: number } }): Promise<StoredObject | null> {
    this.gets.push({ key, range: options?.range });
    const content = this.objects[key];
    if (content == null) {
      return null;
    }

    const bytes = typeof content === "string" ? new TextEncoder().encode(content) : content.slice();

    if (!options?.range) {
      return {
        async text() {
          return typeof content === "string" ? content : new TextDecoder().decode(bytes);
        },
        async arrayBuffer() {
          return bytes.slice().buffer;
        },
      };
    }

    const chunkBytes = bytes.slice(options.range.offset, options.range.offset + options.range.length);
    return {
      async text() {
        return new TextDecoder().decode(chunkBytes);
      },
      async arrayBuffer() {
        return chunkBytes.slice().buffer;
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

function createLookupFixture(cnpj = "00000000000000") {
  const payload = { cnpj, razao_social: "EMPRESA TESTE LTDA" };
  const line = `${JSON.stringify(payload)}\n`;

  return {
    cnpj,
    payload,
    ndjson: line,
    index: buildBinaryIndex([
      { cnpj, offset: 0, length: line.length },
    ]),
  };
}

function createModuleFixture(cnpj: string, payload: Record<string, unknown>) {
  const line = `${JSON.stringify({ cnpj, ...payload })}\n`;

  return {
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

function buildRoutingIndex(entries: Array<{
  cnpj: string;
  references: Array<{
    segmentId: string;
    offset: number;
    length: number;
  }>;
}>): Uint8Array {
  const encoder = new TextEncoder();
  const size = 8 + entries.reduce(
    (total, entry) => total + 16 + entry.references.reduce(
      (referenceTotal, reference) =>
        referenceTotal + 1 + encoder.encode(reference.segmentId).length + 12,
      0,
    ),
    0,
  );
  const index = new Uint8Array(size);
  index.set(encoder.encode("OCR1"), 0);
  const view = new DataView(index.buffer);
  view.setUint32(4, entries.length, true);
  let cursor = 8;

  for (const entry of entries) {
    index.set(encoder.encode(entry.cnpj), cursor);
    cursor += 14;
    view.setUint16(cursor, entry.references.length, true);
    cursor += 2;

    for (const reference of entry.references) {
      const segmentId = encoder.encode(reference.segmentId);
      index[cursor++] = segmentId.length;
      index.set(segmentId, cursor);
      cursor += segmentId.length;
      view.setUint32(cursor, reference.offset >>> 0, true);
      view.setUint32(
        cursor + 4,
        Math.floor(reference.offset / 0x1_0000_0000),
        true,
      );
      view.setUint32(cursor + 8, reference.length, true);
      cursor += 12;
    }
  }

  return index;
}

test.beforeEach(() => {
  __test__.clearHotIndexCache();
  __test__.setEmbeddedRuntimeInfoForTest(null);
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

test("fetch returns a record using binary index and exact R2 range read", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "release-123",
    }),
    "files/shards/releases/release-123/000.index.bin": fixture.index,
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({});

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
  assert.deepEqual(assets.requests, []);
  assert.deepEqual(bucket.gets, [
    {
      key: "files/info.json",
      range: undefined,
    },
    {
      key: "files/shards/releases/release-123/000.index.bin",
      range: undefined,
    },
    {
      key: "files/shards/releases/release-123/000.ndjson",
      range: { offset: 0, length: fixture.ndjson.length },
    },
  ]);
});

test("fetch returns an alphanumeric record from its uppercase shard", async () => {
  const fixture = createLookupFixture("12ABC34501DE35");
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "release-alpha",
    }),
    "files/shards/releases/release-alpha/12A.index.bin": fixture.index,
    "files/shards/releases/release-alpha/12A.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({});

  const response = await worker.fetch(
    new Request("https://worker.invalid/12.abc.345/01de-35"),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), fixture.payload);
  assert.deepEqual(bucket.gets, [
    { key: "files/info.json", range: undefined },
    { key: "files/shards/releases/release-alpha/12A.index.bin", range: undefined },
    {
      key: "files/shards/releases/release-alpha/12A.ndjson",
      range: { offset: 0, length: fixture.ndjson.length },
    },
  ]);
});

test("fetch resolves release from storage_release_id", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "release-current"
    }),
    "files/shards/releases/release-current/000.index.bin": fixture.index,
    "files/shards/releases/release-current/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({});

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
  assert.deepEqual(bucket.gets, [
    {
      key: "files/info.json",
      range: undefined,
    },
    {
      key: "files/shards/releases/release-current/000.index.bin",
      range: undefined,
    },
    {
      key: "files/shards/releases/release-current/000.ndjson",
      range: { offset: 0, length: fixture.ndjson.length },
    },
  ]);
});

test("fetch composes explicitly requested module shards into the base record", async () => {
  const fixture = createLookupFixture();
  const moduleFirstChunk = createModuleFixture(fixture.cnpj, {
    updated_at: "2026-04-14T00:00:00Z",
    obras: [{ cno: "123", nome: "OBRA TESTE" }],
  });
  const moduleSecondChunk = createModuleFixture(fixture.cnpj, {
    updated_at: "2026-04-15T00:00:00Z",
    obras: [{ cno: "456", nome: "SEGUNDA OBRA" }],
  });
  const moduleNdjson =
    moduleFirstChunk.ndjson + moduleSecondChunk.ndjson;
  const moduleIndex = buildBinaryIndex([
    {
      cnpj: fixture.cnpj,
      offset: 0,
      length: moduleFirstChunk.ndjson.length,
    },
    {
      cnpj: fixture.cnpj,
      offset: moduleFirstChunk.ndjson.length,
      length: moduleSecondChunk.ndjson.length,
    },
  ]);
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "base-release",
      datasets: {
        cno: {
          json_property_name: "cno",
          storage_release_id: "cno-release",
        },
      },
    }),
    "files/shards/releases/base-release/000.index.bin": fixture.index,
    "files/shards/releases/base-release/000.ndjson": fixture.ndjson,
    "files/shards/modules/cno/cno-release/000.index.bin": moduleIndex,
    "files/shards/modules/cno/cno-release/000.ndjson": moduleNdjson,
  });
  const assets = new FakeAssetsFetcher({});

  const response = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}?datasets=receita,cno`),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    ...fixture.payload,
    cno: {
      cnpj: fixture.cnpj,
      updated_at: "2026-04-15T00:00:00Z",
      obras: [
        { cno: "123", nome: "OBRA TESTE" },
        { cno: "456", nome: "SEGUNDA OBRA" },
      ],
    },
  });
  assert.deepEqual(bucket.gets, [
    { key: "files/info.json", range: undefined },
    { key: "files/shards/releases/base-release/000.index.bin", range: undefined },
    { key: "files/shards/releases/base-release/000.ndjson", range: { offset: 0, length: fixture.ndjson.length } },
    { key: "files/shards/modules/cno/cno-release/000.index.bin", range: undefined },
    {
      key: "files/shards/modules/cno/cno-release/000.ndjson",
      range: { offset: 0, length: moduleNdjson.length },
    },
  ]);
});

test("fetch composes only routed historical segments for a module", async () => {
  const fixture = createLookupFixture("12ABC34501DE35");
  const segment2017 = createModuleFixture(fixture.cnpj, {
    updated_at: "2017-12-31T00:00:00Z",
    licitacoes: [
      { id: "L-2017", data: "2017-03-10" },
    ],
  });
  const segment2026FirstChunk = createModuleFixture(fixture.cnpj, {
    updated_at: "2026-03-31T00:00:00Z",
    licitacoes: [
      { id: "L-2026", data: "2026-03-12" },
    ],
  });
  const segment2026SecondChunk = createModuleFixture(fixture.cnpj, {
    updated_at: "2026-03-31T00:00:00Z",
    licitacoes: [
      { id: "L-2017", data: "2017-03-10" },
    ],
  });
  const segment2026Ndjson =
    segment2026FirstChunk.ndjson + segment2026SecondChunk.ndjson;
  const routing = buildRoutingIndex([
    {
      cnpj: fixture.cnpj,
      references: [
        {
          segmentId: "2017",
          offset: 0,
          length: segment2017.ndjson.length,
        },
        {
          segmentId: "2026-03",
          offset: 0,
          length: segment2026Ndjson.length,
        },
      ],
    },
  ]);
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      datasets: {
        licitacoes: {
          json_property_name: "licitacoes",
          routing_release_id: "routing-2",
          segment_collection_property: "licitacoes",
          segments: [
            { id: "2017", storage_release_id: "segment-2017" },
            { id: "2020", storage_release_id: "segment-2020" },
            { id: "2026-03", storage_release_id: "segment-2026-03" },
          ],
        },
      },
    }),
    "files/shards/modules/licitacoes/routing/routing-2/12A.routing.bin": routing,
    "files/shards/modules/licitacoes/segments/2017/segment-2017/12A.ndjson":
      segment2017.ndjson,
    "files/shards/modules/licitacoes/segments/2026-03/segment-2026-03/12A.ndjson":
      segment2026Ndjson,
  });

  const response = await worker.fetch(
    new Request(
      `https://worker.invalid/${fixture.cnpj}?datasets=licitacoes`,
    ),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: new FakeAssetsFetcher({}) as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    licitacoes: {
      cnpj: fixture.cnpj,
      updated_at: "2026-03-31T00:00:00Z",
      licitacoes: [
        { id: "L-2017", data: "2017-03-10" },
        { id: "L-2026", data: "2026-03-12" },
      ],
    },
  });
  assert.deepEqual(bucket.gets, [
    { key: "files/info.json", range: undefined },
    {
      key: "files/shards/modules/licitacoes/routing/routing-2/12A.routing.bin",
      range: undefined,
    },
    {
      key:
        "files/shards/modules/licitacoes/segments/2017/segment-2017/12A.ndjson",
      range: { offset: 0, length: segment2017.ndjson.length },
    },
    {
      key:
        "files/shards/modules/licitacoes/segments/2026-03/segment-2026-03/12A.ndjson",
      range: { offset: 0, length: segment2026Ndjson.length },
    },
  ]);
});

test("fetch exposes an explicitly requested module key as null when CNPJ has no module record", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "base-release",
      datasets: {
        cno: {
          json_property_name: "cno",
          storage_release_id: "cno-release",
        },
      },
    }),
    "files/shards/releases/base-release/000.index.bin": fixture.index,
    "files/shards/releases/base-release/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({});

  const response = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}?datasets=receita,cno`),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    ...fixture.payload,
    cno: null,
  });
  assert.deepEqual(bucket.gets, [
    { key: "files/info.json", range: undefined },
    { key: "files/shards/releases/base-release/000.index.bin", range: undefined },
    { key: "files/shards/releases/base-release/000.ndjson", range: { offset: 0, length: fixture.ndjson.length } },
    { key: "files/shards/modules/cno/cno-release/000.index.bin", range: undefined },
  ]);
});

test("fetch with datasets=cno returns only the requested module", async () => {
  const fixture = createLookupFixture();
  const moduleFixture = createModuleFixture(fixture.cnpj, {
    updated_at: "2026-04-14T00:00:00Z",
    obras: [{ cno: "123", nome: "OBRA TESTE" }],
  });
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "base-release",
      datasets: {
        cno: {
          json_property_name: "cno",
          storage_release_id: "cno-release",
        },
      },
    }),
    "files/shards/modules/cno/cno-release/000.index.bin": moduleFixture.index,
    "files/shards/modules/cno/cno-release/000.ndjson": moduleFixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({});

  const response = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}?datasets=cno`),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    cno: {
      cnpj: fixture.cnpj,
      updated_at: "2026-04-14T00:00:00Z",
      obras: [{ cno: "123", nome: "OBRA TESTE" }],
    },
  });
  assert.deepEqual(bucket.gets, [
    { key: "files/info.json", range: undefined },
    { key: "files/shards/modules/cno/cno-release/000.index.bin", range: undefined },
    {
      key: "files/shards/modules/cno/cno-release/000.ndjson",
      range: { offset: 0, length: moduleFixture.ndjson.length },
    },
  ]);
});

test("fetch defaults to the Receita dataset when no dataset is requested", async () => {
  const fixture = createLookupFixture();
  const moduleFixture = createModuleFixture(fixture.cnpj, { nome: "OBRA TESTE" });
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "base-release",
      datasets: {
        cno: {
          json_property_name: "cno",
          storage_release_id: "cno-release",
        },
      },
    }),
    "files/shards/releases/base-release/000.index.bin": fixture.index,
    "files/shards/releases/base-release/000.ndjson": fixture.ndjson,
    "files/shards/modules/cno/cno-release/000.index.bin": moduleFixture.index,
    "files/shards/modules/cno/cno-release/000.ndjson": moduleFixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({});

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
  assert.deepEqual(bucket.gets, [
    { key: "files/info.json", range: undefined },
    { key: "files/shards/releases/base-release/000.index.bin", range: undefined },
    { key: "files/shards/releases/base-release/000.ndjson", range: { offset: 0, length: fixture.ndjson.length } },
  ]);
});

test("fetch with embedded runtime info skips info.json and uses asset Receita index", async () => {
  const fixture = createLookupFixture();
  __test__.setEmbeddedRuntimeInfoForTest({
    storage_release_id: "base-release",
  });

  const bucket = new FakeBucket({
    "files/shards/releases/base-release/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
    "/files/shards/000.index.bin": fixture.index,
  });

  const response = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}?datasets=receita`),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), fixture.payload);
  assert.deepEqual(assets.requests, ["/files/shards/000.index.bin"]);
  assert.deepEqual(bucket.gets, [
    { key: "files/shards/releases/base-release/000.ndjson", range: { offset: 0, length: fixture.ndjson.length } },
  ]);
});

test("fetch with embedded runtime info uses module asset indexes", async () => {
  const fixture = createLookupFixture();
  const moduleFixture = createModuleFixture(fixture.cnpj, {
    updated_at: "2026-04-14T00:00:00Z",
    obras: [{ cno: "123", nome: "OBRA TESTE" }],
  });
  __test__.setEmbeddedRuntimeInfoForTest({
    storage_release_id: "base-release",
    datasets: {
      cno: {
        json_property_name: "cno",
        storage_release_id: "cno-release",
      },
    },
  });

  const bucket = new FakeBucket({
    "files/shards/modules/cno/cno-release/000.ndjson": moduleFixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
    "/files/shards/modules/cno/cno-release/000.index.bin": moduleFixture.index,
  });

  const response = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}?datasets=cno`),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    cno: {
      cnpj: fixture.cnpj,
      updated_at: "2026-04-14T00:00:00Z",
      obras: [{ cno: "123", nome: "OBRA TESTE" }],
    },
  });
  assert.deepEqual(assets.requests, ["/files/shards/modules/cno/cno-release/000.index.bin"]);
  assert.deepEqual(bucket.gets, [
    {
      key: "files/shards/modules/cno/cno-release/000.ndjson",
      range: { offset: 0, length: moduleFixture.ndjson.length },
    },
  ]);
});

test("fetch rejects unknown datasets", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "base-release",
      datasets: {
        cno: {
          json_property_name: "cno",
          storage_release_id: "cno-release",
        },
      },
    }),
  });
  const assets = new FakeAssetsFetcher({});

  const response = await worker.fetch(
    new Request(`https://worker.invalid/${fixture.cnpj}?datasets=antt`),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 400);
  assert.deepEqual(await response.json(), { error: "invalid dataset: antt" });
  assert.deepEqual(bucket.gets, [
    { key: "files/info.json", range: undefined },
  ]);
});

test("fetch canonicalizes masked URLs in cache", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "release-123",
    }),
    "files/shards/releases/release-123/000.index.bin": fixture.index,
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({});
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
  assert.equal(assets.requests.length, 0);
  assert.equal(bucket.gets.length, 3);
});

test("fetch falls back to the legacy asset index when R2 index is missing", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "release-123",
    }),
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
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
  assert.deepEqual(assets.requests, ["/files/shards/000.index.bin"]);
  assert.deepEqual(bucket.gets, [
    {
      key: "files/info.json",
      range: undefined,
    },
    {
      key: "files/shards/releases/release-123/000.index.bin",
      range: undefined,
    },
    {
      key: "files/shards/releases/release-123/000.ndjson",
      range: { offset: 0, length: fixture.ndjson.length },
    },
  ]);
});

test("fetch returns 404 when the record is not present in the binary index", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "release-123",
    }),
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
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
  assert.deepEqual(bucket.gets, [
    {
      key: "files/info.json",
      range: undefined,
    },
    {
      key: "files/shards/releases/release-123/000.index.bin",
      range: undefined,
    },
  ]);
});

test("fetch returns JSON Schema 2020-12 on /schema", async () => {
  const bucket = new FakeBucket({});
  const assets = new FakeAssetsFetcher({});

  const response = await worker.fetch(
    new Request("https://worker.invalid/schema"),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.match(response.headers.get("Content-Type") ?? "", /application\/json/);
  assert.equal(response.headers.get("Cache-Control"), "no-store");

  const schema = (await response.json()) as Record<string, unknown>;
  assert.equal(schema["$schema"], "https://json-schema.org/draft/2020-12/schema");
  assert.equal(schema["$id"], "https://api.opencnpj.org/schema");
  assert.equal(schema.type, "object");

  const required = schema.required as string[];
  for (const field of [
    "cnpj",
    "razao_social",
    "cnae_principal",
    "natureza_juridica",
    "cep",
    "uf",
    "municipio",
    "telefones",
    "QSA",
  ]) {
    assert.ok(required.includes(field), `expected required to include ${field}`);
  }

  const defs = schema["$defs"] as Record<string, unknown>;
  for (const def of [
    "Telefone",
    "QsaMember",
    "CnoPayload",
    "CnoObra",
    "CodigoDescricao",
    "RntrcPayload",
    "PortalFavorecidosPjPayload",
    "PortalSancoesPayload",
    "PortalCepimPayload",
    "PortalAcordosLenienciaPayload",
    "PortalLicitacoesPayload",
    "PortalContratosPayload",
    "PortalRenunciasPayload",
    "PortalNotasFiscaisPayload",
    "PortalConveniosPayload",
    "PortalEmendasPayload",
    "PortalEmendasDocumentosPayload",
  ]) {
    assert.ok(defs[def], `expected $defs to include ${def}`);
  }

  const properties = schema.properties as Record<string, { type?: string; oneOf?: Array<{ type?: string; $ref?: string }> }>;
  assert.equal(properties.cnpj?.type, "string");
  assert.deepEqual(
    properties.rntrc?.oneOf,
    [
      { type: "null" },
      { $ref: "#/$defs/RntrcPayload" },
    ],
  );
  for (const [key, definition] of [
    ["favorecidos_pj", "PortalFavorecidosPjPayload"],
    ["ceis", "PortalSancoesPayload"],
    ["cepim", "PortalCepimPayload"],
    ["cnep", "PortalSancoesPayload"],
    ["acordos_leniencia", "PortalAcordosLenienciaPayload"],
    ["licitacoes", "PortalLicitacoesPayload"],
    ["contratos", "PortalContratosPayload"],
    ["renuncias_fiscais", "PortalRenunciasPayload"],
    ["notas_fiscais", "PortalNotasFiscaisPayload"],
    ["convenios", "PortalConveniosPayload"],
    ["emendas_parlamentares", "PortalEmendasPayload"],
    ["emendas_documentos", "PortalEmendasDocumentosPayload"],
  ]) {
    assert.deepEqual(
      properties[key]?.oneOf,
      [
        { type: "null" },
        { $ref: `#/$defs/${definition}` },
      ],
    );

    const payloadDefinition = defs[definition] as Record<string, unknown>;
    assert.ok(
      !(payloadDefinition.required as string[]).includes("cnpj"),
      definition + " must keep CNPJ as the outer lookup key",
    );
  }

  const fixture = createLookupFixture();
  for (const key of Object.keys(fixture.payload)) {
    assert.ok(key in properties, `fixture key ${key} should be declared in schema.properties`);
  }

  assert.deepEqual(bucket.gets, []);
  assert.deepEqual(assets.requests, []);
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
  assert.equal(response.headers.get("Cache-Control"), "no-store");
  assert.deepEqual(await response.json(), { versao: "2026-03" });
  assert.deepEqual(bucket.gets, [{ key: "files/info.json", range: undefined }]);
});

test("fetch /info serves embedded runtime info without caching", async () => {
  __test__.setEmbeddedRuntimeInfoForTest({
    storage_release_id: "base-release",
    datasets: {
      rntrc: {
        json_property_name: "rntrc",
        storage_release_id: "rntrc-release",
      },
    },
  });

  const bucket = new FakeBucket({});
  const assets = new FakeAssetsFetcher({});

  const response = await worker.fetch(
    new Request("https://worker.invalid/info"),
    {
      CNPJ_BUCKET: bucket as unknown as R2Bucket,
      ASSETS: assets as unknown as Fetcher,
    } satisfies Env,
    createExecutionContext(),
  );

  assert.equal(response.status, 200);
  assert.equal(response.headers.get("Cache-Control"), "no-store");
  assert.deepEqual(await response.json(), {
    storage_release_id: "base-release",
    datasets: {
      rntrc: {
        json_property_name: "rntrc",
        storage_release_id: "rntrc-release",
      },
    },
  });
});

test("fetch reuses hot chunk cache when response cache is cold but range is already hot", async () => {
  const fixture = createLookupFixture();
  const bucket = new FakeBucket({
    "files/info.json": JSON.stringify({
      storage_release_id: "release-123",
    }),
    "files/shards/releases/release-123/000.ndjson": fixture.ndjson,
  });
  const assets = new FakeAssetsFetcher({
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
  assert.equal(assets.requests.length, 1);
  assert.deepEqual(bucket.gets, [
    { key: "files/info.json", range: undefined },
    { key: "files/shards/releases/release-123/000.index.bin", range: undefined },
    { key: "files/shards/releases/release-123/000.ndjson", range: { offset: 0, length: fixture.ndjson.length } },
    { key: "files/shards/releases/release-123/000.index.bin", range: undefined },
  ]);
});
