export function isRecord(value: unknown) {
  return !!value && typeof value === "object";
}

export function toErrorMessage(error: unknown) {
  if (error instanceof Error) return error.message;
  if (typeof error === "string") return error;
  return "unknown_error";
}
