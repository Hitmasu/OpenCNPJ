import assert from "node:assert/strict";
import test from "node:test";

import { clearHotCaches, getHotIndex, rememberHotIndex } from "./cache.ts";
import { HOT_INDEX_CACHE_MAX_BYTES } from "./constants.ts";

test("hot index cache does not retain an index larger than its byte budget", () => {
  clearHotCaches();

  const buffer = new ArrayBuffer(HOT_INDEX_CACHE_MAX_BYTES + 1);
  rememberHotIndex("oversized", {
    recordCount: 0,
    bytes: new Uint8Array(buffer),
    view: new DataView(buffer),
  });

  assert.equal(getHotIndex("oversized"), null);
  clearHotCaches();
});
