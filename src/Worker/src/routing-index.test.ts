import assert from "node:assert/strict";
import test from "node:test";

import {
  findSegmentRoutingReferences,
  parseSegmentRoutingIndex,
} from "./routing-index.ts";

test("parseSegmentRoutingIndex reads variable segment references", () => {
  const index = parseSegmentRoutingIndex(
    buildRoutingIndex([
      {
        cnpj: "12ABC34501DE35",
        references: [
          { segmentId: "2017", offset: 10, length: 50 },
          { segmentId: "2026-03", offset: 80, length: 90 },
        ],
      },
    ]).buffer,
    "test.routing.bin",
  );

  assert.equal(index.recordCount, 1);
  assert.deepEqual(
    findSegmentRoutingReferences(index, "12ABC34501DE35"),
    [
      { segmentId: "2017", offset: 10, length: 50 },
      { segmentId: "2026-03", offset: 80, length: 90 },
    ],
  );
  assert.deepEqual(
    findSegmentRoutingReferences(index, "00000000000000"),
    [],
  );
});

test("parseSegmentRoutingIndex rejects truncated references", () => {
  const valid = buildRoutingIndex([
    {
      cnpj: "00000000000000",
      references: [{ segmentId: "2026", offset: 0, length: 10 }],
    },
  ]);

  assert.throws(
    () => parseSegmentRoutingIndex(
      valid.slice(0, valid.length - 1).buffer,
      "truncated.routing.bin",
    ),
    /truncated entry/,
  );
});

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
  const bytes = new Uint8Array(size);
  bytes.set(encoder.encode("OCR1"), 0);
  const view = new DataView(bytes.buffer);
  view.setUint32(4, entries.length, true);
  let cursor = 8;

  for (const entry of entries) {
    bytes.set(encoder.encode(entry.cnpj), cursor);
    cursor += 14;
    view.setUint16(cursor, entry.references.length, true);
    cursor += 2;

    for (const reference of entry.references) {
      const segmentBytes = encoder.encode(reference.segmentId);
      bytes[cursor++] = segmentBytes.length;
      bytes.set(segmentBytes, cursor);
      cursor += segmentBytes.length;
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

  return bytes;
}
