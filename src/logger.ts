import type { RabbitMqLogMeta, RabbitMqLogger } from "./types.js";

const noop = (_meta: RabbitMqLogMeta, _msg: string) => {
  // no-op logger by default
};

export function normalizeLogger(logger?: RabbitMqLogger) {
  return {
    debug: logger?.debug ?? noop,
    info: logger?.info ?? noop,
    warn: logger?.warn ?? noop,
    error: logger?.error ?? noop,
  };
}
