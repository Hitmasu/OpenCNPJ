import {
  CNPJ_LENGTH,
  ROUTING_INDEX_HEADER_SIZE,
  ROUTING_INDEX_MAGIC,
} from "./constants.ts";
import type {
  SegmentRoutingIndex,
  SegmentRoutingReference,
} from "./types.ts";

const textDecoder = new TextDecoder("utf-8", { fatal: true });

export function parseSegmentRoutingIndex(
  buffer: ArrayBuffer,
  path: string,
): SegmentRoutingIndex {
  const bytes = new Uint8Array(buffer);
  if (bytes.byteLength < ROUTING_INDEX_HEADER_SIZE) {
    throw new Error(`[routing-index:${path}] header too small`);
  }

  for (let index = 0; index < ROUTING_INDEX_MAGIC.length; index++) {
    if (bytes[index] !== ROUTING_INDEX_MAGIC.charCodeAt(index)) {
      throw new Error(`[routing-index:${path}] invalid magic`);
    }
  }

  const view = new DataView(buffer, bytes.byteOffset, bytes.byteLength);
  const recordCount = view.getUint32(4, true);
  const entries = new Map<string, SegmentRoutingReference[]>();
  let cursor = ROUTING_INDEX_HEADER_SIZE;

  for (let recordIndex = 0; recordIndex < recordCount; recordIndex++) {
    ensureAvailable(bytes, cursor, CNPJ_LENGTH + 2, path);
    const cnpj = decode(bytes.subarray(cursor, cursor + CNPJ_LENGTH), path);
    cursor += CNPJ_LENGTH;

    const referenceCount = view.getUint16(cursor, true);
    cursor += 2;
    const references: SegmentRoutingReference[] = [];

    for (let referenceIndex = 0; referenceIndex < referenceCount; referenceIndex++) {
      ensureAvailable(bytes, cursor, 1, path);
      const segmentIdLength = bytes[cursor++];
      if (segmentIdLength === 0) {
        throw new Error(`[routing-index:${path}] empty segment id`);
      }

      ensureAvailable(bytes, cursor, segmentIdLength + 12, path);
      const segmentId = decode(
        bytes.subarray(cursor, cursor + segmentIdLength),
        path,
      );
      cursor += segmentIdLength;

      const low = view.getUint32(cursor, true);
      const high = view.getUint32(cursor + 4, true);
      const offset = (high * 0x1_0000_0000) + low;
      const length = view.getUint32(cursor + 8, true);
      cursor += 12;

      if (!Number.isSafeInteger(offset)) {
        throw new Error(`[routing-index:${path}] unsafe offset`);
      }

      references.push({ segmentId, offset, length });
    }

    if (entries.has(cnpj)) {
      throw new Error(`[routing-index:${path}] duplicate CNPJ ${cnpj}`);
    }

    entries.set(cnpj, references);
  }

  if (cursor !== bytes.byteLength) {
    throw new Error(
      `[routing-index:${path}] trailing bytes ${bytes.byteLength - cursor}`,
    );
  }

  return { recordCount, bytes, entries };
}

export function findSegmentRoutingReferences(
  index: SegmentRoutingIndex,
  cnpj: string,
): SegmentRoutingReference[] {
  return index.entries.get(cnpj) ?? [];
}

function ensureAvailable(
  bytes: Uint8Array,
  cursor: number,
  required: number,
  path: string,
): void {
  if (cursor + required > bytes.byteLength) {
    throw new Error(`[routing-index:${path}] truncated entry`);
  }
}

function decode(bytes: Uint8Array, path: string): string {
  try {
    return textDecoder.decode(bytes);
  } catch (error) {
    throw new Error(`[routing-index:${path}] invalid UTF-8`, {
      cause: error,
    });
  }
}
