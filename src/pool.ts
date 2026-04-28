import amqp from "amqplib";
import type { ConsumeMessage } from "amqplib";
import { toErrorMessage } from "./guards.js";
import { normalizeLogger } from "./logger.js";
import type {
  BindingOptions,
  ConsumeOptions,
  ExchangeOptions,
  ExchangeType,
  PublishOptions,
  QueueOptions,
  RabbitMQConfig,
  RabbitMqLogger,
  RabbitMQPoolContract,
  RabbitMQPoolEntry,
} from "./types.js";

type RegisteredConsumer = {
  queue: string;
  onMessage: (msg: ConsumeMessage | null) => void;
  options?: ConsumeOptions & { prefetch?: number };
};

export class RabbitMQPool implements RabbitMQPoolContract {
  private static instances = new Map<string, RabbitMQPool>();
  private readonly config: RabbitMQConfig;
  private readonly namespace: string;
  private readonly logger: ReturnType<typeof normalizeLogger>;
  private entry: RabbitMQPoolEntry;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private readonly reconnectDelayMin = 1000;
  private readonly reconnectDelayMax = 30000;
  private shuttingDown = false;
  private connectPromise: Promise<void> | null = null;
  private recoveringFromConsumerCancel = false;
  private channelVersion = 0;
  private readonly registeredExchanges = new Map<string, { type: ExchangeType; options?: ExchangeOptions }>();
  private readonly registeredQueues = new Map<string, QueueOptions | undefined>();
  private readonly registeredBindings = new Map<
    string,
    { queue: string; exchange: string; routingKey: string; options?: BindingOptions }
  >();
  private readonly registeredConsumers = new Map<string, RegisteredConsumer>();
  private readonly pausedConsumerTimers = new Map<string, NodeJS.Timeout>();
  private readonly pausingConsumers = new Set<string>();
  private readonly locallyCancelledConsumers = new Set<string>();
  private consumerSeq = 0;

  private constructor(namespace: string, config: RabbitMQConfig, logger?: RabbitMqLogger) {
    this.namespace = namespace;
    this.config = config;
    this.logger = normalizeLogger(logger);
    this.entry = {
      connection: null,
      channel: null,
      connecting: false,
      lastError: null,
      retryCount: 0,
    };
  }

  static getInstance(namespace: string, config: RabbitMQConfig, logger?: RabbitMqLogger) {
    if (!this.instances.has(namespace)) {
      this.instances.set(namespace, new RabbitMQPool(namespace, config, logger));
    }

    const instance = this.instances.get(namespace);
    if (!instance) {
      throw new Error(`[rabbitmq] failed to create namespace '${namespace}'`);
    }

    return instance;
  }

  private bindingKey(queue: string, exchange: string, routingKey: string) {
    return `${queue}::${exchange}::${routingKey}`;
  }

  private sleep(ms: number) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  private buildConnectionUrl() {
    const { protocol, hostname, port, username, password, vhost, heartbeat, frameMax, channelMax, locale } = this.config;

    const params = new URLSearchParams();
    if (heartbeat) params.append("heartbeat", String(heartbeat));
    if (frameMax) params.append("frameMax", String(frameMax));
    if (channelMax) params.append("channelMax", String(channelMax));
    if (locale) params.append("locale", locale);

    const query = params.toString();
    const qs = query ? `?${query}` : "";

    return `${protocol}://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${hostname}:${port}/${encodeURIComponent(vhost)}${qs}`;
  }

  private async recoverFromConsumerCancel(queue: string, consumerTag: string) {
    if (this.shuttingDown || this.recoveringFromConsumerCancel) return;

    if (this.locallyCancelledConsumers.has(consumerTag)) {
      this.locallyCancelledConsumers.delete(consumerTag);
      this.logger.info({ namespace: this.namespace, queue, consumerTag }, "[rabbitmq] consumer cancelled locally");
      return;
    }

    this.recoveringFromConsumerCancel = true;

    try {
      this.logger.warn(
        { namespace: this.namespace, queue, consumerTag },
        "[rabbitmq] consumer cancelled by broker, scheduling reconnect"
      );

      const channel = this.entry.channel;
      this.entry.channel = null;

      if (channel) {
        try {
          await channel.close();
        } catch {
          // channel may already be closed
        }
      }
    } finally {
      this.recoveringFromConsumerCancel = false;
      this.scheduleReconnect();
    }
  }

  private createAmqpOptions() {
    const connectionName = this.config.connectionName?.trim() || this.namespace;

    if (this.config.ssl?.enabled) {
      return {
        ca: this.config.ssl.ca,
        cert: this.config.ssl.cert,
        key: this.config.ssl.key,
        rejectUnauthorized: this.config.ssl.rejectUnauthorized ?? false,
        clientProperties: { connection_name: connectionName },
      };
    }

    return {
      clientProperties: { connection_name: connectionName },
    };
  }

  async connect() {
    if (this.config.enabled === false) return;

    this.shuttingDown = false;

    if (this.entry.connection && this.entry.channel) {
      return;
    }

    if (this.connectPromise) {
      await this.connectPromise;
      return;
    }

    this.connectPromise = (async () => {
      this.entry.connecting = true;

      try {
        if (!this.entry.connection) {
          const url = this.buildConnectionUrl();
          const options = this.createAmqpOptions();

          this.logger.info({ namespace: this.namespace }, "[rabbitmq] connecting...");
          this.entry.connection = await amqp.connect(url, options);

          this.entry.connection.on("error", (err: Error) => {
            this.entry.lastError = err;
            this.logger.error({ namespace: this.namespace, error: err.message }, "[rabbitmq] connection error");
          });

          this.entry.connection.on("close", () => {
            this.logger.warn({ namespace: this.namespace }, "[rabbitmq] connection closed");
            this.entry.connection = null;
            this.entry.channel = null;
            if (!this.shuttingDown) {
              this.scheduleReconnect();
            }
          });
        }

        if (!this.entry.connection) {
          throw new Error("[rabbitmq] connection not available");
        }

        if (!this.entry.channel) {
          this.entry.channel = await this.entry.connection.createConfirmChannel();
          this.channelVersion += 1;

          this.entry.channel.on("error", (err: Error) => {
            this.entry.lastError = err;
            this.logger.error({ namespace: this.namespace, error: err.message }, "[rabbitmq] channel error");
          });

          this.entry.channel.on("close", () => {
            this.logger.warn({ namespace: this.namespace }, "[rabbitmq] channel closed");
            this.entry.channel = null;
            if (!this.shuttingDown) {
              this.scheduleReconnect();
            }
          });
        }

        await this.restoreTopology();
        await this.restoreRegisteredConsumers();

        this.entry.retryCount = 0;
        this.entry.connecting = false;
        this.logger.info({ namespace: this.namespace }, "[rabbitmq] connected");
      } catch (error) {
        this.entry.connecting = false;
        this.entry.lastError = error instanceof Error ? error : new Error(toErrorMessage(error));
        this.logger.error({ namespace: this.namespace, error: toErrorMessage(error) }, "[rabbitmq] connection failed");

        if (!this.shuttingDown) {
          this.scheduleReconnect();
        }

        throw error;
      } finally {
        this.connectPromise = null;
      }
    })();

    await this.connectPromise;
  }

  private async restoreRegisteredConsumers() {
    if (this.registeredConsumers.size === 0) return;

    const channel = this.entry.channel;
    if (!channel) return;

    let failedCount = 0;

    for (const [consumerTag, c] of this.registeredConsumers.entries()) {
      try {
        if (c.options?.prefetch) {
          await channel.prefetch(c.options.prefetch);
        }

        await channel.consume(c.queue, c.onMessage, {
          consumerTag,
          noLocal: c.options?.noLocal ?? false,
          noAck: c.options?.noAck ?? false,
          exclusive: c.options?.exclusive ?? false,
          priority: c.options?.priority,
          arguments: c.options?.arguments,
        });

        this.logger.info({ namespace: this.namespace, queue: c.queue, consumerTag }, "[rabbitmq] consumer re-subscribed");
      } catch (error) {
        failedCount += 1;
        this.logger.error(
          {
            namespace: this.namespace,
            queue: c.queue,
            consumerTag,
            error: toErrorMessage(error),
          },
          "[rabbitmq] failed to re-subscribe consumer"
        );
      }
    }

    if (failedCount > 0) {
      throw new Error(`[rabbitmq] failed to re-subscribe ${failedCount} consumer(s)`);
    }
  }

  private async restoreTopology() {
    const channel = this.entry.channel;
    if (!channel) return;

    for (const [exchange, def] of this.registeredExchanges.entries()) {
      await channel.assertExchange(exchange, def.type, {
        durable: def.options?.durable ?? true,
        autoDelete: def.options?.autoDelete ?? false,
        internal: def.options?.internal ?? false,
        arguments: def.options?.arguments,
      });
    }

    for (const [queue, options] of this.registeredQueues.entries()) {
      const args: Record<string, unknown> = { ...(options?.arguments ?? {}) };
      if (options?.maxLength !== undefined) args["x-max-length"] = options.maxLength;
      if (options?.deadLetterExchange !== undefined) args["x-dead-letter-exchange"] = options.deadLetterExchange;
      if (options?.deadLetterRoutingKey !== undefined) args["x-dead-letter-routing-key"] = options.deadLetterRoutingKey;

      await channel.assertQueue(queue, {
        durable: options?.durable ?? true,
        exclusive: options?.exclusive ?? false,
        autoDelete: options?.autoDelete ?? false,
        arguments: args,
      });
    }

    for (const [, def] of this.registeredBindings.entries()) {
      await channel.bindQueue(def.queue, def.exchange, def.routingKey, def.options?.arguments);
    }
  }

  private scheduleReconnect() {
    if (this.shuttingDown || this.reconnectTimeout) {
      return;
    }

    this.entry.retryCount += 1;
    const delay = Math.min(this.reconnectDelayMin * 2 ** Math.max(0, this.entry.retryCount - 1), this.reconnectDelayMax);

    this.reconnectTimeout = setTimeout(() => {
      this.reconnectTimeout = null;
      this.logger.info({ namespace: this.namespace, attempt: this.entry.retryCount }, "[rabbitmq] reconnecting...");
      void this.connect().catch(() => {
        // connect already logs
      });
    }, delay);
  }

  async getChannel() {
    if (!this.entry.channel) {
      await this.connect();
    }

    if (!this.entry.channel) {
      throw new Error("[rabbitmq] channel is not available");
    }

    return this.entry.channel;
  }

  async close() {
    this.shuttingDown = true;
    this.connectPromise = null;

    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    for (const timer of this.pausedConsumerTimers.values()) {
      clearTimeout(timer);
    }

    this.pausedConsumerTimers.clear();
    this.pausingConsumers.clear();

    try {
      if (this.entry.channel) {
        await this.entry.channel.close();
        this.entry.channel = null;
      }

      if (this.entry.connection) {
        await this.entry.connection.close();
        this.entry.connection = null;
      }

      this.logger.info({ namespace: this.namespace }, "[rabbitmq] closed");
    } catch (error) {
      this.logger.error({ namespace: this.namespace, error: toErrorMessage(error) }, "[rabbitmq] error during close");
    }
  }

  async assertExchange(exchange: string, type: ExchangeType, options?: ExchangeOptions) {
    this.registeredExchanges.set(exchange, { type, options });

    const channel = await this.getChannel();
    await channel.assertExchange(exchange, type, {
      durable: options?.durable ?? true,
      autoDelete: options?.autoDelete ?? false,
      internal: options?.internal ?? false,
      arguments: options?.arguments,
    });

    this.logger.info({ namespace: this.namespace, exchange, type }, "[rabbitmq] exchange asserted");
  }

  async assertQueue(queue: string, options?: QueueOptions) {
    this.registeredQueues.set(queue, options);

    const channel = await this.getChannel();
    const args: Record<string, unknown> = { ...(options?.arguments ?? {}) };

    if (options?.maxLength !== undefined) args["x-max-length"] = options.maxLength;
    if (options?.deadLetterExchange !== undefined) args["x-dead-letter-exchange"] = options.deadLetterExchange;
    if (options?.deadLetterRoutingKey !== undefined) args["x-dead-letter-routing-key"] = options.deadLetterRoutingKey;

    const result = await channel.assertQueue(queue, {
      durable: options?.durable ?? true,
      exclusive: options?.exclusive ?? false,
      autoDelete: options?.autoDelete ?? false,
      arguments: args,
    });

    this.logger.info({ namespace: this.namespace, queue }, "[rabbitmq] queue asserted");

    return result;
  }

  async deleteQueue(queue: string, options?: { ifUnused?: boolean; ifEmpty?: boolean }) {
    const channel = await this.getChannel();

    const result = await channel.deleteQueue(queue, {
      ifUnused: options?.ifUnused ?? false,
      ifEmpty: options?.ifEmpty ?? false,
    });

    this.logger.info({ namespace: this.namespace, queue }, "[rabbitmq] queue deleted");

    return result;
  }

  async cleanupEmptyRetryQueues(opts?: { retryDelaysMs?: number[] }) {
    const retryRegex = /\.retry(?:\.\d+ms)?$/;
    const retryQueues = new Set<string>();
    const delays = (opts?.retryDelaysMs ?? []).filter((n) => Number.isInteger(n) && n > 0);

    for (const queue of this.registeredQueues.keys()) {
      if (retryRegex.test(queue)) {
        retryQueues.add(queue);
        continue;
      }

      retryQueues.add(`${queue}.retry`);

      for (const delay of delays) {
        retryQueues.add(`${queue}.retry.${delay}ms`);
      }
    }

    let deleted = 0;
    let kept = 0;

    for (const queue of retryQueues) {
      try {
        await this.deleteQueue(queue, { ifEmpty: true });
        this.registeredQueues.delete(queue);
        deleted += 1;
      } catch (error) {
        const message = toErrorMessage(error);

        if (message.includes("NOT_FOUND")) {
          continue;
        }

        kept += 1;
        this.logger.debug({ namespace: this.namespace, queue, error: message }, "[rabbitmq] retry queue kept");
      }
    }

    return { total: retryQueues.size, deleted, kept };
  }

  async bindQueue(queue: string, exchange: string, routingKey: string, options?: BindingOptions) {
    this.registeredBindings.set(this.bindingKey(queue, exchange, routingKey), { queue, exchange, routingKey, options });

    const channel = await this.getChannel();
    await channel.bindQueue(queue, exchange, routingKey, options?.arguments);

    this.logger.info({ namespace: this.namespace, queue, exchange, routingKey }, "[rabbitmq] queue bound");
  }

  async publish(exchange: string, routingKey: string, content: Buffer, options?: PublishOptions) {
    const maxAttempts = 3;
    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        const channel = await this.getChannel();

        const published = channel.publish(exchange, routingKey, content, {
          persistent: options?.persistent ?? true,
          contentType: options?.contentType ?? "application/json",
          contentEncoding: options?.contentEncoding,
          headers: options?.headers,
          priority: options?.priority,
          correlationId: options?.correlationId,
          replyTo: options?.replyTo,
          messageId: options?.messageId,
          timestamp: options?.timestamp,
          type: options?.type,
          userId: options?.userId,
          appId: options?.appId,
        });

        if (!published) {
          this.logger.warn({ namespace: this.namespace, exchange, routingKey }, "[rabbitmq] publish backpressure");
        }

        await channel.waitForConfirms();

        return published;
      } catch (error) {
        const message = toErrorMessage(error);
        lastError = error instanceof Error ? error : new Error(message);
        this.entry.lastError = lastError;
        this.entry.channel = null;

        this.logger.warn(
          {
            namespace: this.namespace,
            exchange,
            routingKey,
            attempt,
            maxAttempts,
            error: message,
          },
          "[rabbitmq] publish failed, retrying"
        );

        if (attempt >= maxAttempts || this.shuttingDown) {
          throw lastError;
        }

        try {
          await this.connect();
        } catch {
          // connect logs and schedules reconnection
        }

        await this.sleep(Math.min(200 * 2 ** (attempt - 1), 1000));
      }
    }

    throw lastError ?? new Error("[rabbitmq] publish failed");
  }

  async consume(
    queue: string,
    onMessage: (msg: ConsumeMessage | null) => void,
    options?: ConsumeOptions & { prefetch?: number }
  ) {
    const channel = await this.getChannel();

    const consumerTag = options?.consumerTag ?? `${this.namespace}.consumer.${++this.consumerSeq}`;

    const wrappedOnMessage = (msg: ConsumeMessage | null) => {
      if (!msg) {
        void this.recoverFromConsumerCancel(queue, consumerTag);
        return onMessage(msg);
      }

      (msg as unknown as { __rabbitChannelVersion?: number }).__rabbitChannelVersion = this.channelVersion;
      return onMessage(msg);
    };

    if (options?.prefetch) {
      await channel.prefetch(options.prefetch);
    }

    const result = await channel.consume(queue, wrappedOnMessage, {
      consumerTag,
      noLocal: options?.noLocal ?? false,
      noAck: options?.noAck ?? false,
      exclusive: options?.exclusive ?? false,
      priority: options?.priority,
      arguments: options?.arguments,
    });

    this.registeredConsumers.set(consumerTag, {
      queue,
      onMessage: wrappedOnMessage,
      options: { ...options, consumerTag },
    });

    this.logger.info({ namespace: this.namespace, queue, consumerTag: result.consumerTag }, "[rabbitmq] consumer started");

    return result;
  }

  async cancelConsume(consumerTag: string) {
    const channel = await this.getChannel();

    this.locallyCancelledConsumers.add(consumerTag);
    await channel.cancel(consumerTag);

    this.registeredConsumers.delete(consumerTag);

    const timer = this.pausedConsumerTimers.get(consumerTag);
    if (timer) {
      clearTimeout(timer);
      this.pausedConsumerTimers.delete(consumerTag);
    }

    this.pausingConsumers.delete(consumerTag);

    this.logger.info({ namespace: this.namespace, consumerTag }, "[rabbitmq] consumer cancelled");
  }

  async pauseConsumer(consumerTag: string, delayMs: number) {
    if (delayMs <= 0 || this.shuttingDown) return;
    if (this.pausedConsumerTimers.has(consumerTag)) return;
    if (this.pausingConsumers.has(consumerTag)) return;

    this.pausingConsumers.add(consumerTag);

    const registered = this.registeredConsumers.get(consumerTag);
    if (!registered) {
      this.pausingConsumers.delete(consumerTag);
      return;
    }

    try {
      const channel = await this.getChannel();
      this.locallyCancelledConsumers.add(consumerTag);
      await channel.cancel(consumerTag);

      this.logger.warn({ namespace: this.namespace, consumerTag, delayMs }, "[rabbitmq] consumer paused");
    } catch (error) {
      this.logger.error(
        { namespace: this.namespace, consumerTag, error: toErrorMessage(error) },
        "[rabbitmq] failed to pause consumer"
      );

      this.pausingConsumers.delete(consumerTag);
      return;
    }

    const timer = setTimeout(async () => {
      this.pausedConsumerTimers.delete(consumerTag);

      if (this.shuttingDown) return;

      const channel = this.entry.channel;
      const current = this.registeredConsumers.get(consumerTag);
      if (!channel || !current) {
        this.scheduleReconnect();
        return;
      }

      try {
        if (current.options?.prefetch) {
          await channel.prefetch(current.options.prefetch);
        }

        await channel.consume(current.queue, current.onMessage, {
          consumerTag,
          noLocal: current.options?.noLocal ?? false,
          noAck: current.options?.noAck ?? false,
          exclusive: current.options?.exclusive ?? false,
          priority: current.options?.priority,
          arguments: current.options?.arguments,
        });

        this.logger.info({ namespace: this.namespace, consumerTag, queue: current.queue }, "[rabbitmq] consumer resumed");
      } catch (error) {
        this.logger.error(
          { namespace: this.namespace, consumerTag, error: toErrorMessage(error) },
          "[rabbitmq] failed to resume consumer"
        );

        this.scheduleReconnect();
      } finally {
        this.pausingConsumers.delete(consumerTag);
      }
    }, delayMs);

    this.pausedConsumerTimers.set(consumerTag, timer);
  }

  async ack(message: amqp.Message, allUpTo?: boolean) {
    const messageVersion = (message as unknown as { __rabbitChannelVersion?: number }).__rabbitChannelVersion;

    if (typeof messageVersion === "number" && messageVersion !== this.channelVersion) {
      this.logger.warn(
        { namespace: this.namespace, messageVersion, channelVersion: this.channelVersion },
        "[rabbitmq] skip ack on stale channel"
      );

      return;
    }

    const channel = await this.getChannel();

    try {
      channel.ack(message, allUpTo);
    } catch (error) {
      this.logger.error({ namespace: this.namespace, error: toErrorMessage(error) }, "[rabbitmq] ack failed");
    }
  }

  async nack(message: amqp.Message, allUpTo?: boolean, requeue?: boolean) {
    const messageVersion = (message as unknown as { __rabbitChannelVersion?: number }).__rabbitChannelVersion;

    if (typeof messageVersion === "number" && messageVersion !== this.channelVersion) {
      this.logger.warn(
        { namespace: this.namespace, messageVersion, channelVersion: this.channelVersion },
        "[rabbitmq] skip nack on stale channel"
      );

      return;
    }

    const channel = await this.getChannel();

    try {
      channel.nack(message, allUpTo, requeue);
    } catch (error) {
      this.logger.error({ namespace: this.namespace, error: toErrorMessage(error) }, "[rabbitmq] nack failed");
    }
  }

  async reject(message: amqp.Message, requeue?: boolean) {
    const messageVersion = (message as unknown as { __rabbitChannelVersion?: number }).__rabbitChannelVersion;

    if (typeof messageVersion === "number" && messageVersion !== this.channelVersion) {
      this.logger.warn(
        { namespace: this.namespace, messageVersion, channelVersion: this.channelVersion },
        "[rabbitmq] skip reject on stale channel"
      );

      return;
    }

    const channel = await this.getChannel();

    try {
      channel.reject(message, requeue);
    } catch (error) {
      this.logger.error({ namespace: this.namespace, error: toErrorMessage(error) }, "[rabbitmq] reject failed");
    }
  }
}
