import assert from "node:assert/strict";
import test from "node:test";

import { resolveDatasetSelection } from "./datasets.ts";
import type { RuntimeInfo } from "./types.ts";

const runtimeInfo: RuntimeInfo = {
  datasets: {
    receita: {},
    rntrc: {},
    cno: {},
  },
};

test("dataset selection defaults to Receita only", () => {
  const result = resolveDatasetSelection(new URLSearchParams(), runtimeInfo);

  assert.deepEqual(result, {
    ok: true,
    value: {
      includeReceita: true,
      moduleKeys: [],
      cacheKey: "receita",
    },
  });
});

test("dataset selection canonicalizes the internal cache key", () => {
  const result = resolveDatasetSelection(
    new URLSearchParams("datasets=rntrc,receita,cno"),
    runtimeInfo,
  );

  assert.deepEqual(result, {
    ok: true,
    value: {
      includeReceita: true,
      moduleKeys: ["cno", "rntrc"],
      cacheKey: "receita,cno,rntrc",
    },
  });
});
