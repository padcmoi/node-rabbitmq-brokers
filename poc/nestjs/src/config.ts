export const appConfig = {
  port: Number(process.env.NEST_PORT ?? 4020),
  rabbitmq: {
    protocol: (process.env.RABBITMQ_PROTOCOL ?? "amqp") as "amqp" | "amqps",
    hostname: process.env.RABBITMQ_HOST ?? "127.0.0.1",
    port: Number(process.env.RABBITMQ_PORT ?? 5672),
    username: process.env.RABBITMQ_USER ?? "guest",
    password: process.env.RABBITMQ_PASSWORD ?? "guest",
    vhost: process.env.RABBITMQ_VHOST ?? "/",
    heartbeat: 30,
    enabled: true,
  },
  redisUrl: process.env.REDIS_URL ?? "redis://127.0.0.1:6391",
  hmac: {
    namespace: process.env.HMAC_NAMESPACE ?? "poc:rabbitmq:messages",
    secretToken: process.env.HMAC_SECRET_TOKEN ?? "poc_hmac_secret_token",
    credentials: [
      { clientId: "express_app", secret: "express_secret" },
      { clientId: "nest_app", secret: "nest_secret" },
    ],
  },
  broker: {
    topicExchange: "poc.bus.topic",
    fanoutExchange: "poc.bus.fanout",
    queue: "poc.nest.queue",
    bindingKey: "poc.nest.#",
    unknownTypeRetryDelayMs: 3000,
    publishRoutingKeyToExpress: "poc.express.events",
  },
};
