import type { ConsumeMessage } from "amqplib";
import { toErrorMessage } from "./guards.js";
import { normalizeLogger } from "./logger.js";
import { RabbitMQPool } from "./pool.js";
import type {
  ExchangeOptions,
  ExchangeType,
  HmacSignedRabbitMQMessage,
  MessageHandler,
  PublishOptions,
  QueueOptions,
  RabbitMQConfig,
  RabbitMQConsumerOptions,
  RabbitMQHmacServices,
  RabbitMqLogger,
  RabbitMQProducerOptions,
  SignedHmacMessage,
} from "./types.js";

export class RabbitMQClient {
  private readonly pool: RabbitMQPool;
  private readonly namespace: string;
  private readonly hmac?: RabbitMQHmacServices;
  private readonly logger: ReturnType<typeof normalizeLogger>;

  constructor(namespace: string, config: RabbitMQConfig, options?: { hmac?: RabbitMQHmacServices; logger?: RabbitMqLogger }) {
    this.namespace = namespace;
    this.hmac = options?.hmac;
    this.logger = normalizeLogger(options?.logger);
    this.pool = RabbitMQPool.getInstance(namespace, config, options?.logger);
  }

  private retryQueueName(queue: string) {
    return `${queue}.retry`;
  }

  private async deferMessage(queue: string, msg: ConsumeMessage, delayMs: number) {
    const retryQueue = this.retryQueueName(queue);

    await this.pool.assertQueue(retryQueue, {
      durable: true,
      arguments: {
        "x-message-ttl": delayMs,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": queue,
      },
    });

    const published = await this.pool.publish("", retryQueue, msg.content, {
      persistent: true,
      contentType: msg.properties.contentType as string | undefined,
      contentEncoding: msg.properties.contentEncoding as string | undefined,
      headers: msg.properties.headers,
      priority: msg.properties.priority as number | undefined,
      correlationId: msg.properties.correlationId as string | undefined,
      replyTo: msg.properties.replyTo as string | undefined,
      messageId: msg.properties.messageId as string | undefined,
      timestamp: msg.properties.timestamp as number | undefined,
      type: msg.properties.type as string | undefined,
      userId: msg.properties.userId as string | undefined,
      appId: msg.properties.appId as string | undefined,
    });

    return published;
  }

  connect() {
    return this.pool.connect();
  }

  close() {
    return this.pool.close();
  }

  assertExchange(exchange: string, type: ExchangeType, options?: ExchangeOptions) {
    return this.pool.assertExchange(exchange, type, options);
  }

  assertQueue(queue: string, options?: QueueOptions) {
    return this.pool.assertQueue(queue, options);
  }

  cleanupEmptyRetryQueues(opts?: { retryDelaysMs?: number[] }) {
    return this.pool.cleanupEmptyRetryQueues(opts);
  }

  bindQueue(queue: string, exchange: string, routingKey: string) {
    return this.pool.bindQueue(queue, exchange, routingKey);
  }

  async publish<TPayload = unknown>(options: RabbitMQProducerOptions, payload: TPayload) {
    if (!this.hmac?.signMessage) {
      throw new Error("[rabbitmq] missing hmac.signMessage service");
    }
    const signMessage = this.hmac.signMessage;

    const signed = await signMessage(options.hmacKeyId, payload);

    if ("ok" in signed && signed.ok === false) {
      throw new Error(`[rabbitmq] sign failed: ${signed.message ?? signed.code ?? "unknown_sign_error"}`);
    }

    const envelope: HmacSignedRabbitMQMessage<TPayload> = {
      meta: {
        keyId: signed.meta.keyId,
        tsMs: signed.meta.tsMs,
        nonce: signed.meta.nonce,
        sig: signed.meta.sig,
      },
      payload,
    };

    const content = Buffer.from(JSON.stringify(envelope), "utf-8");

    const publishOptions: PublishOptions = {
      ...options.publishOptions,
      contentType: "application/json",
      persistent: options.publishOptions?.persistent ?? true,
      timestamp: Date.now(),
      headers: {
        ...(options.publishOptions?.headers ?? {}),
        "x-hmac-keyid": signed.meta.keyId,
      },
    };

    const published = await this.pool.publish(options.exchange, options.routingKey ?? "", content, publishOptions);

    this.logger.info(
      {
        namespace: this.namespace,
        exchange: options.exchange,
        routingKey: options.routingKey,
        keyId: signed.meta.keyId,
      },
      "[rabbitmq] published signed message"
    );

    return published;
  }

  async consume<TPayload = unknown>(options: RabbitMQConsumerOptions, handler: MessageHandler<TPayload>) {
    if (!this.hmac?.verifyMessage) {
      throw new Error("[rabbitmq] missing hmac.verifyMessage service");
    }
    const verifyMessage = this.hmac.verifyMessage;

    const prefetch = Math.max(1, Math.trunc(options.prefetchCount ?? 1));

    const consumer = await this.pool.consume(
      options.queue,
      async (msg: ConsumeMessage | null) => {
        if (!msg) {
          this.logger.warn({ namespace: this.namespace, queue: options.queue }, "[rabbitmq] null message received");
          return;
        }

        try {
          const raw = msg.content.toString("utf-8");
          const parsed = JSON.parse(raw) as unknown;

          if (!parsed || typeof parsed !== "object") {
            this.logger.error({ namespace: this.namespace, queue: options.queue }, "[rabbitmq] invalid message envelope");
            await this.pool.reject(msg, false);
            return;
          }

          const envelope = parsed as Partial<HmacSignedRabbitMQMessage<TPayload>>;
          if (!envelope.meta || !envelope.meta.keyId || !envelope.meta.sig) {
            this.logger.error(
              { namespace: this.namespace, queue: options.queue },
              "[rabbitmq] missing hmac metadata in envelope"
            );
            await this.pool.reject(msg, false);
            return;
          }

          if (options.allowedKeyIds && options.allowedKeyIds.length > 0) {
            if (!options.allowedKeyIds.includes(envelope.meta.keyId)) {
              this.logger.warn(
                {
                  namespace: this.namespace,
                  queue: options.queue,
                  keyId: envelope.meta.keyId,
                  allowedKeyIds: options.allowedKeyIds,
                },
                "[rabbitmq] keyId not allowed"
              );

              await this.pool.reject(msg, false);
              return;
            }
          }

          const signedForVerify: SignedHmacMessage<TPayload> = {
            meta: {
              keyId: envelope.meta.keyId,
              tsMs: envelope.meta.tsMs ?? Date.now(),
              nonce: envelope.meta.nonce ?? "",
              sig: envelope.meta.sig,
            },
            payload: envelope.payload as TPayload,
          };

          const verification = await verifyMessage(signedForVerify);

          if (!verification.ok) {
            const requeueOnFail = verification.requeueOnFail ?? false;
            const requeueDelayMs = Math.max(0, verification.requeueDelayMs ?? 0);

            this.logger.error(
              {
                namespace: this.namespace,
                queue: options.queue,
                keyId: envelope.meta.keyId,
                code: verification.code,
                error: verification.message,
                requeueOnFail,
                requeueDelayMs,
              },
              "[rabbitmq] hmac verification failed"
            );

            if (requeueOnFail && requeueDelayMs > 0) {
              try {
                await this.deferMessage(options.queue, msg, requeueDelayMs);
                await this.pool.ack(msg);
                return;
              } catch (error) {
                this.logger.error(
                  {
                    namespace: this.namespace,
                    queue: options.queue,
                    keyId: envelope.meta.keyId,
                    delayMs: requeueDelayMs,
                    error: toErrorMessage(error),
                  },
                  "[rabbitmq] failed to defer invalid message"
                );
              }
            }

            await this.pool.reject(msg, requeueOnFail);
            return;
          }

          await handler(envelope.payload as TPayload, {
            fields: {
              consumerTag: msg.fields.consumerTag,
              deliveryTag: msg.fields.deliveryTag,
              redelivered: msg.fields.redelivered,
              exchange: msg.fields.exchange,
              routingKey: msg.fields.routingKey,
            },
            properties: {
              contentType: msg.properties.contentType as string | undefined,
              contentEncoding: msg.properties.contentEncoding as string | undefined,
              headers: msg.properties.headers,
              deliveryMode: msg.properties.deliveryMode as number | undefined,
              priority: msg.properties.priority as number | undefined,
              correlationId: msg.properties.correlationId as string | undefined,
              replyTo: msg.properties.replyTo as string | undefined,
              expiration: msg.properties.expiration as string | undefined,
              messageId: msg.properties.messageId as string | undefined,
              timestamp: msg.properties.timestamp as number | undefined,
              type: msg.properties.type as string | undefined,
              userId: msg.properties.userId as string | undefined,
              appId: msg.properties.appId as string | undefined,
            },
            content: msg.content,
            ack: () => this.pool.ack(msg),
            nack: (allUpTo?: boolean, requeue?: boolean) => this.pool.nack(msg, allUpTo, requeue),
            reject: (requeue?: boolean) => this.pool.reject(msg, requeue),
            defer: async (delayMs: number) => {
              const safeDelayMs = Math.max(0, Math.trunc(delayMs));

              if (safeDelayMs === 0) {
                await this.pool.reject(msg, true);
                return;
              }

              await this.deferMessage(options.queue, msg, safeDelayMs);
              await this.pool.ack(msg);
            },
          });

          if (options.autoAck) {
            await this.pool.ack(msg);
          }
        } catch (error) {
          this.logger.error(
            {
              namespace: this.namespace,
              queue: options.queue,
              error: toErrorMessage(error),
            },
            "[rabbitmq] error while consuming message"
          );

          await this.pool.reject(msg, true);
        }
      },
      {
        ...options.consumeOptions,
        prefetch,
        noAck: false,
      }
    );

    this.logger.info(
      {
        namespace: this.namespace,
        queue: options.queue,
        consumerTag: consumer.consumerTag,
        prefetch,
      },
      "[rabbitmq] consumer started"
    );

    return consumer;
  }
}
