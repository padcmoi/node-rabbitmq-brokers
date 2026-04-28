import { isRecord, toErrorMessage } from "./guards.js";
import { normalizeLogger } from "./logger.js";
import type {
  BrokerActionPublishInput,
  BrokerPayload,
  MessageContext,
  RabbitMqBrokerActionContract,
  RabbitMqBrokerRouterOptions,
  RabbitMQProducerOptions,
} from "./types.js";

function normalizeActionType(rawActionType: string) {
  const raw = rawActionType.trim();
  if (!raw) return null;
  if (raw.includes("..")) return null;

  let value = raw.replace(/\\/g, "/");
  value = value.replace(/^[./]+/, "");

  const actionsIndex = value.lastIndexOf("/actions/");
  if (actionsIndex >= 0) {
    value = value.slice(actionsIndex + "/actions/".length);
  } else if (value.startsWith("actions/")) {
    value = value.slice("actions/".length);
  }

  value = value.replace(/\.(ts|js)$/, "");
  value = value.replace(/\/+/g, "/");
  value = value.replace(/^\/+|\/+$/g, "");

  if (!value || value.includes("..") || !/^[A-Za-z0-9/_-]+$/.test(value)) {
    return null;
  }

  return value;
}

function parsePayloadData(data?: string) {
  if (!data) return null;

  try {
    const parsed = JSON.parse(data) as unknown;
    if (!isRecord(parsed)) return null;
    return parsed;
  } catch {
    return null;
  }
}

function toRoutingKeys(routingKeys: string[]) {
  return routingKeys
    .filter((value) => typeof value === "string")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
}

export class RabbitMqBrokerRouter {
  private readonly options: RabbitMqBrokerRouterOptions;
  private readonly logger: ReturnType<typeof normalizeLogger>;
  private topologyReady = false;
  private consumerStarted = false;
  private readonly actionHandlers = new Map<string, RabbitMqBrokerActionContract<Record<string, unknown>>>();

  constructor(options: RabbitMqBrokerRouterOptions) {
    this.options = options;
    this.logger = normalizeLogger(options.logger);
  }

  registerAction<TPayload extends object>(action: RabbitMqBrokerActionContract<TPayload>) {
    const normalized = normalizeActionType(action.type);
    if (!normalized) {
      throw new Error(`[rabbitmq-router] invalid action.type '${action.type}'`);
    }

    this.actionHandlers.set(normalized, action as RabbitMqBrokerActionContract<Record<string, unknown>>);
    return this;
  }

  private async ensureTopology() {
    await this.options.client.connect();
    if (this.topologyReady) return;

    await this.options.client.assertExchange(this.options.topicExchange, "topic", { durable: true });
    await this.options.client.assertExchange(this.options.fanoutExchange, "fanout", { durable: true });
    await this.options.client.assertQueue(this.options.queue, { durable: true });

    await this.options.client.bindQueue(this.options.queue, this.options.topicExchange, this.options.bindingKey);
    await this.options.client.bindQueue(this.options.queue, this.options.fanoutExchange, "");

    this.topologyReady = true;
  }

  async publish<TPayload extends object>(
    hmacKeyId: string,
    action: RabbitMqBrokerActionContract<TPayload>,
    input: BrokerActionPublishInput<TPayload>,
    routingKeys: string[] = []
  ) {
    if (!input.message || typeof input.message !== "string") {
      return {
        published: false,
        err: { status: 400, code: "bad_payload_message", message: "payload.message is required" },
      };
    }

    if (!input.payload || typeof input.payload !== "object" || Array.isArray(input.payload)) {
      return {
        published: false,
        err: { status: 400, code: "bad_payload_object", message: "payload.payload must be an object" },
      };
    }

    const normalizedType = normalizeActionType(action.type);
    if (!normalizedType) {
      return {
        published: false,
        err: { status: 400, code: "bad_payload_type", message: `invalid payload.type '${action.type}'` },
      };
    }

    await this.ensureTopology();

    const payload = {
      ...action.publish(input),
      type: normalizedType,
    } satisfies BrokerPayload;

    const cleanRoutingKeys = toRoutingKeys(routingKeys);

    if (cleanRoutingKeys.length === 0) {
      const options: RabbitMQProducerOptions = {
        exchange: this.options.fanoutExchange,
        routingKey: "",
        hmacKeyId,
        publishOptions: { persistent: true, timestamp: Date.now() },
      };

      const published = await this.options.client.publish(options, payload);
      return { published: published ?? false };
    }

    const results = await Promise.all(
      cleanRoutingKeys.map(async (routingKey) => {
        const options: RabbitMQProducerOptions = {
          exchange: this.options.topicExchange,
          routingKey,
          hmacKeyId,
          publishOptions: { persistent: true, timestamp: Date.now() },
        };

        return await this.options.client.publish(options, payload);
      })
    );

    return { published: results.every(Boolean) };
  }

  private async deferUnknownTypeMessage(message: MessageContext, incomingType: string) {
    this.logger.warn(
      {
        type: incomingType,
        routingKey: message.fields.routingKey,
        deliveryTag: message.fields.deliveryTag,
        requeueDelayMs: this.options.unknownTypeRetryDelayMs,
      },
      "[rabbitmq-router] unknown type, message deferred"
    );

    await message.defer(this.options.unknownTypeRetryDelayMs);
  }

  async startConsumer(consumeOptions?: { allowedKeyIds?: string[]; prefetchCount?: number }) {
    if (this.consumerStarted) return;

    await this.ensureTopology();

    await this.options.client.consume<BrokerPayload>(
      {
        queue: this.options.queue,
        allowedKeyIds: consumeOptions?.allowedKeyIds,
        prefetchCount: consumeOptions?.prefetchCount ?? 1,
        autoAck: false,
      },
      async (payload, message) => {
        const incomingType = typeof payload.type === "string" ? payload.type : "";
        const normalizedType = normalizeActionType(incomingType);
        const action = this.actionHandlers.get(normalizedType ?? incomingType);

        if (!action) {
          await this.deferUnknownTypeMessage(message, incomingType);
          return;
        }

        const parsedData = parsePayloadData(payload.data);

        try {
          await action.consume(payload, parsedData as Record<string, unknown> | null);
          message.ack();
        } catch (error) {
          this.logger.error(
            {
              type: incomingType,
              routingKey: message.fields.routingKey,
              deliveryTag: message.fields.deliveryTag,
              error: toErrorMessage(error),
            },
            "[rabbitmq-router] handler failed"
          );

          message.nack(false, true);
        }
      }
    );

    this.consumerStarted = true;

    this.logger.info({ queue: this.options.queue }, "[rabbitmq-router] consumer started");
  }
}

export const brokerRouterHelpers = {
  normalizeActionType,
  parsePayloadData,
};
