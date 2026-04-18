import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const workerRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const target = resolve(workerRoot, "src/generated-runtime-info.ts");

if (!existsSync(target)) {
  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, [
    "import type { RuntimeInfo } from \"./types.ts\";",
    "",
    "export const GENERATED_RUNTIME_INFO = null as RuntimeInfo | null;",
    "",
    "let runtimeInfoForTest: RuntimeInfo | null | undefined;",
    "",
    "export function getEmbeddedRuntimeInfo(): RuntimeInfo | null {",
    "  if (runtimeInfoForTest !== undefined) {",
    "    return runtimeInfoForTest;",
    "  }",
    "",
    "  return GENERATED_RUNTIME_INFO;",
    "}",
    "",
    "export function hasEmbeddedRuntimeInfo(): boolean {",
    "  return getEmbeddedRuntimeInfo() != null;",
    "}",
    "",
    "export function setEmbeddedRuntimeInfoForTest(value: RuntimeInfo | null | undefined): void {",
    "  runtimeInfoForTest = value;",
    "}",
    "",
  ].join("\n"));
}
