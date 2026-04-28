import type { Channel, ChannelModel, ConfirmChannel, ConsumeMessage, Message, Replies } from "amqplib";

export type RabbitMqLogMeta = Record<string, unknown>;

export type RabbitMqLogger = {
  debug?: (meta: RabbitMqLogMeta, msg: string) => void;
  info?: (meta: RabbitMqLogMeta, msg: string) => void;
  warn?: (meta: RabbitMqLogMeta, msg: string) => void;
  error?: (meta: RabbitMqLogMeta, msg: string) => void;
};

export type RabbitMQConfig = {
  protocol: "amqp" | "amqps";
  hostname: string;
  port: number;
  username: string;
  password: string;
  vhost: string;
  heartbeat?: number;
  frameMax?: number;
  channelMax?: number;
  locale?: string;
  connectionName?: string;
  enabled?: boolean;
  ssl?: {
    enabled?: boolean;
    rejectUnauthorized?: boolean;
    ca?: Buffer[] | string[];
    cert?: Buffer | string;
    key?: Buffer | string;
  };
};

export type ExchangeType = "direct" | "topic" | "fanout" | "headers";

export type ExchangeOptions = {
  durable?: boolean;
  autoDelete?: boolean;
  internal?: boolean;
  arguments?: Record<string, unknown>;
};

export type QueueOptions = {
  durable?: boolean;
  exclusive?: boolean;
  autoDelete?: boolean;
  arguments?: Record<string, unknown>;
  maxLength?: number;
  deadLetterExchange?: string;
  deadLetterRoutingKey?: string;
};

export type BindingOptions = {
  arguments?: Record<string, unknown>;
};

export type PublishOptions = {
  persistent?: boolean;
  contentType?: string;
  contentEncoding?: string;
  headers?: Record<string, unknown>;
  priority?: number;
  correlationId?: string;
  replyTo?: string;
  messageId?: string;
  timestamp?: number;
  type?: string;
  userId?: string;
  appId?: string;
};

export type ConsumeOptions = {
  consumerTag?: string;
  noLocal?: boolean;
  noAck?: boolean;
  exclusive?: boolean;
  priority?: number;
  arguments?: Record<string, unknown>;
};

export type MessageContext = {
  fields: {
    consumerTag: string;
    deliveryTag: number;
    redelivered: boolean;
    exchange: string;
    routingKey: string;
  };
  properties: {
    contentType?: string;
    contentEncoding?: string;
    headers?: Record<string, unknown>;
    deliveryMode?: number;
    priority?: number;
    correlationId?: string;
    replyTo?: string;
    expiration?: string;
    messageId?: string;
    timestamp?: number;
    type?: string;
    userId?: string;
    appId?: string;
  };
  content: Buffer;
  ack: () => void;
  nack: (allUpTo?: boolean, requeue?: boolean) => void;
  reject: (requeue?: boolean) => void;
  defer: (delayMs: number) => Promise<void>;
};

export type MessageHandler<T = unknown> = (payload: T, message: MessageContext) => Promise<void> | void;

export type RabbitMQPoolEntry = {
  connection: ChannelModel | null;
  channel: ConfirmChannel | null;
  connecting: boolean;
  lastError: Error | null;
  retryCount: number;
};

export type HmacSignedRabbitMQMessage<TPayload = unknown> = {
  meta: {
    keyId: string;
    tsMs: number;
    nonce: string;
    sig: string;
  };
  payload: TPayload;
};

export type SignedHmacMessage<TPayload = unknown> = HmacSignedRabbitMQMessage<TPayload> & {
  ok?: false;
  code?: string;
  message?: string;
};

export type RabbitMQProducerOptions = {
  exchange: string;
  routingKey?: string;
  hmacKeyId: string;
  publishOptions?: PublishOptions;
};

export type RabbitMQConsumerOptions = {
  queue: string;
  allowedKeyIds?: string[];
  consumeOptions?: ConsumeOptions;
  autoAck?: boolean;
  prefetchCount?: number;
};

export type RabbitMQVerifyMessageResult = {
  ok: boolean;
  code?: string;
  message?: string;
  requeueOnFail?: boolean;
  requeueDelayMs?: number;
};

export type RabbitMQHmacServices = {
  signMessage: (keyId: string, payload: unknown) => Promise<SignedHmacMessage>;
  verifyMessage: (msg: SignedHmacMessage) => Promise<RabbitMQVerifyMessageResult>;
};

export type RabbitMQPoolContract = {
  connect: () => Promise<void>;
  getChannel: () => Promise<Channel>;
  close: () => Promise<void>;
  assertExchange: (exchange: string, type: ExchangeType, options?: ExchangeOptions) => Promise<void>;
  assertQueue: (queue: string, options?: QueueOptions) => Promise<Replies.AssertQueue>;
  deleteQueue: (queue: string, options?: { ifUnused?: boolean; ifEmpty?: boolean }) => Promise<Replies.DeleteQueue>;
  cleanupEmptyRetryQueues: (opts?: { retryDelaysMs?: number[] }) => Promise<{ total: number; deleted: number; kept: number }>;
  bindQueue: (queue: string, exchange: string, routingKey: string, options?: BindingOptions) => Promise<void>;
  publish: (exchange: string, routingKey: string, content: Buffer, options?: PublishOptions) => Promise<boolean>;
  consume: (
    queue: string,
    onMessage: (msg: ConsumeMessage | null) => void,
    options?: ConsumeOptions & { prefetch?: number }
  ) => Promise<Replies.Consume>;
  cancelConsume: (consumerTag: string) => Promise<void>;
  pauseConsumer: (consumerTag: string, delayMs: number) => Promise<void>;
  ack: (message: Message, allUpTo?: boolean) => Promise<void>;
  nack: (message: Message, allUpTo?: boolean, requeue?: boolean) => Promise<void>;
  reject: (message: Message, requeue?: boolean) => Promise<void>;
};

export type BrokerActionPublishInput<TPayload extends object> = {
  message: string;
  payload: TPayload;
};

export type BrokerActionRequestData<TPayload extends object> = {
  payload: BrokerActionPublishInput<TPayload>;
  routingKeys: string[];
};

export type BrokerActionRequestValidation<TPayload extends object> =
  | {
      ok: true;
      data: BrokerActionRequestData<TPayload>;
    }
  | {
      ok: false;
      err: { status: number; code: string; message: string };
    };

export type BrokerPayload = {
  type?: string;
  data?: string;
  message?: string;
  timestamp?: number;
};

export type RabbitMqBrokerActionContract<TPayload extends object> = {
  type: string;
  publish: (input: BrokerActionPublishInput<TPayload>) => BrokerPayload;
  consume: (message: BrokerPayload, payload: TPayload | null) => Promise<void> | void;
};

export type RabbitMqBrokerRouterOptions = {
  client: {
    connect: () => Promise<void>;
    assertExchange: (exchange: string, type: ExchangeType, options?: ExchangeOptions) => Promise<void>;
    assertQueue: (queue: string, options?: QueueOptions) => Promise<unknown>;
    bindQueue: (queue: string, exchange: string, routingKey: string) => Promise<void>;
    publish: <TPayload>(options: RabbitMQProducerOptions, payload: TPayload) => Promise<boolean>;
    consume: <TPayload>(
      options: RabbitMQConsumerOptions,
      handler: (payload: TPayload, message: MessageContext) => Promise<void> | void
    ) => Promise<unknown>;
  };
  queue: string;
  bindingKey: string;
  topicExchange: string;
  fanoutExchange: string;
  unknownTypeRetryDelayMs: number;
  logger?: RabbitMqLogger;
};
