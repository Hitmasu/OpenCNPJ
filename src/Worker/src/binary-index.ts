import {
  CNPJ_LENGTH,
  INDEX_ENTRY_SIZE,
  INDEX_HEADER_SIZE,
  INDEX_LENGTH_OFFSET,
  INDEX_MAGIC,
  INDEX_OFFSET_OFFSET,
} from "./constants.ts";
import type { BinaryIndexEntry, BinaryShardIndex } from "./types.ts";

const textEncoder = new TextEncoder();

export function parseBinaryShardIndex(buffer: ArrayBuffer, assetPath: string): BinaryShardIndex {
  const bytes = new Uint8Array(buffer);
  if (bytes.byteLength < INDEX_HEADER_SIZE) {
    throw new Error(`[binary-index:${assetPath}] header too small`);
  }

  for (let i = 0; i < INDEX_MAGIC.length; i++) {
    if (bytes[i] !== INDEX_MAGIC.charCodeAt(i)) {
      throw new Error(`[binary-index:${assetPath}] invalid magic`);
    }
  }

  const view = new DataView(buffer, bytes.byteOffset, bytes.byteLength);
  const recordCount = view.getUint32(4, true);
  const expectedSize = INDEX_HEADER_SIZE + (recordCount * INDEX_ENTRY_SIZE);
  if (bytes.byteLength !== expectedSize) {
    throw new Error(`[binary-index:${assetPath}] invalid size ${bytes.byteLength}, expected ${expectedSize}`);
  }

  return {
    recordCount,
    bytes,
    view,
  };
}

export function findBinaryIndexEntry(index: BinaryShardIndex, cnpj: string): BinaryIndexEntry | null {
  const target = textEncoder.encode(cnpj);
  if (target.length !== CNPJ_LENGTH) {
    return null;
  }

  let low = 0;
  let high = index.recordCount - 1;

  while (low <= high) {
    const mid = (low + high) >> 1;
    const entryStart = INDEX_HEADER_SIZE + (mid * INDEX_ENTRY_SIZE);
    const compareResult = compareIndexedCnpj(index.bytes, entryStart, target);

    if (compareResult === 0) {
      return readBinaryIndexEntry(index.view, entryStart);
    }

    if (compareResult < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return null;
}

function compareIndexedCnpj(bytes: Uint8Array, entryStart: number, target: Uint8Array): number {
  for (let index = 0; index < CNPJ_LENGTH; index++) {
    const diff = bytes[entryStart + index] - target[index];
    if (diff !== 0) {
      return diff;
    }
  }

  return 0;
}

function readBinaryIndexEntry(view: DataView, entryStart: number): BinaryIndexEntry {
  const low = view.getUint32(entryStart + INDEX_OFFSET_OFFSET, true);
  const high = view.getUint32(entryStart + INDEX_OFFSET_OFFSET + 4, true);
  const offset = (high * 0x1_0000_0000) + low;
  const length = view.getUint32(entryStart + INDEX_LENGTH_OFFSET, true);

  return { offset, length };
}

