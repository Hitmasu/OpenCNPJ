export function createStageError(stage: string, error: unknown): Error {
  if (error instanceof Error) {
    return new Error(`[${stage}] ${error.message}`, { cause: error });
  }

  return new Error(`[${stage}] ${typeof error === "string" ? error : JSON.stringify(error)}`);
}

