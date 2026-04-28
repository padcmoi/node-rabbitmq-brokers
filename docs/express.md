# Express Integration

This guide shows how to integrate `@naskot/node-rabbitmq-brokers` in an Express API with a dedicated service layer.

## 1) Install

```bash
npm install @naskot/node-rabbitmq-brokers @naskot/node-hmac-auth amqplib express redis
```

## 2) Complete Service Setup

Recommended split:

- `src/services/hmac.service.ts`
- `src/services/rabbitmq.service.ts`
- `src/actions/*.action.ts`

### `src/services/hmac.service.ts`

```ts
import { randomUUID } from "node:crypto";
import { createClient, type RedisClientType } from "redis";
import { initializeHmacMessageAuth, type InitializedHmacMessageAuth } from "@naskot/node-hmac-auth";
import type { SignedHmacMessage } from "@naskot/node-rabbitmq-brokers";

let redisClient: RedisClientType | null = null;
let hmacMessageAuth: InitializedHmacMessageAuth | null = null;
let bootstrapPromise: Promise<void> | null = null;

export async function bootstrapHmacService() {
  if (bootstrapPromise) {
    await bootstrapPromise;
    return;
  }

  bootstrapPromise = (async () => {
    redisClient = createClient({ url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379" });
    await redisClient.connect();

    hmacMessageAuth = initializeHmacMessageAuth({
      redis: redisClient,
      namespace: process.env.HMAC_NAMESPACE ?? "my-api-messages",
      secretToken: process.env.HMAC_SECRET_TOKEN,
    });

    const defaults = [
      { clientId: "express_app", secret: "express_secret" },
      { clientId: "nest_app", secret: "nest_secret" },
    ];

    for (const credential of defaults) {
      const existing = await hmacMessageAuth.clients.get(credential.clientId);
      if (existing) continue;

      await hmacMessageAuth.clients.create({
        clientId: credential.clientId,
        plainSecret: credential.secret,
      });
    }
  })();

  await bootstrapPromise;
}

function getMessageAuth() {
  if (!hmacMessageAuth) {
    throw new Error("hmac message auth is not initialized");
  }

  return hmacMessageAuth;
}

export async function signHmacMessage(clientId: string, payload: unknown) {
  await bootstrapHmacService();

  const signed = await getMessageAuth().signMessage({
    clientId,
    message: payload,
  });

  return {
    meta: {
      keyId: signed.clientId,
      tsMs: Date.now(),
      nonce: randomUUID(),
      sig: signed.signature,
    },
    payload,
  } satisfies SignedHmacMessage;
}

export async function verifyHmacMessage(message: SignedHmacMessage) {
  await bootstrapHmacService();

  try {
    await getMessageAuth().verifyMessage({
      clientId: message.meta.keyId,
      message: message.payload,
      signature: message.meta.sig,
    });

    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      code: "hmac_verify_failed",
      message: error instanceof Error ? error.message : "hmac verification failed",
      requeueOnFail: true,
      requeueDelayMs: 3000,
    };
  }
}
```

### `src/services/rabbitmq.service.ts`

```ts
import { RabbitMQClient, RabbitMqBrokerRouter } from "@naskot/node-rabbitmq-brokers";
import { createOutboundAction, type PingPayload } from "../actions/outbound.action.js";
import { createInboundAction } from "../actions/inbound.action.js";
import { signHmacMessage, verifyHmacMessage } from "./hmac.service.js";

const rabbitClient = new RabbitMQClient(
  "MY_EXPRESS_APP",
  {
    protocol: "amqp",
    hostname: process.env.RABBITMQ_HOST ?? "127.0.0.1",
    port: Number(process.env.RABBITMQ_PORT ?? 5672),
    username: process.env.RABBITMQ_USER ?? "guest",
    password: process.env.RABBITMQ_PASSWORD ?? "guest",
    vhost: process.env.RABBITMQ_VHOST ?? "/",
    heartbeat: 30,
  },
  {
    hmac: {
      signMessage: signHmacMessage,
      verifyMessage: verifyHmacMessage,
    },
  }
);

const brokerRouter = new RabbitMqBrokerRouter({
  client: rabbitClient,
  queue: "my.express.queue",
  bindingKey: "my.express.#",
  topicExchange: "my.bus.topic",
  fanoutExchange: "my.bus.fanout",
  unknownTypeRetryDelayMs: 3000,
});

const outboundAction = createOutboundAction();
const inboundAction = createInboundAction(async (payload) => {
  await publishToNest({
    kind: "pong",
    text: `pong for ${payload.threadId}`,
    threadId: payload.threadId,
  });
});

brokerRouter.registerAction(inboundAction);

export async function bootstrapRabbitMqService() {
  await brokerRouter.startConsumer({
    allowedKeyIds: ["nest_app"],
    prefetchCount: 5,
  });
}

export async function publishToNest(input: { kind: "ping" | "pong"; text: string; threadId: string }) {
  const payload: PingPayload = {
    from: "express",
    kind: input.kind,
    text: input.text,
    threadId: input.threadId,
    at: Date.now(),
  };

  return await brokerRouter.publish(
    "express_app",
    outboundAction,
    {
      message: `[express] ${input.kind}`,
      payload,
    },
    ["my.nest.events"]
  );
}
```

## 3) Helpers and Responsibilities

Recommended helper responsibilities:

- `bootstrapHmacService()`: initialize Redis + HMAC runtime once
- `signHmacMessage()`: adapter from your payload to broker signed envelope
- `verifyHmacMessage()`: adapter from broker signed envelope to HMAC verification result
- `bootstrapRabbitMqService()`: start topology and consumers once
- `publishToNest()`: provide a stable app-level publish helper

## 4) Actions and Folder Layout

Suggested layout:

```text
src/
  actions/
    inbound.action.ts
    outbound.action.ts
  services/
    hmac.service.ts
    rabbitmq.service.ts
  app.ts
```

Action contract rules:

- each action must define a stable `type`
- `publish(...)` maps business payload -> broker payload
- `consume(...)` handles incoming message + parsed payload
- keep transport logic in actions, orchestration in services

## 5) Express App Usage

```ts
import express from "express";
import { randomUUID } from "node:crypto";
import { bootstrapHmacService } from "./services/hmac.service.js";
import { bootstrapRabbitMqService, publishToNest } from "./services/rabbitmq.service.js";

async function bootstrap() {
  await bootstrapHmacService();
  await bootstrapRabbitMqService();

  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.status(200).json({ ok: true, app: "express" });
  });

  app.post("/publish/to-nest", async (req, res) => {
    const text = typeof req.body?.text === "string" && req.body.text.trim() ? req.body.text.trim() : "ping from express";

    const result = await publishToNest({
      kind: "ping",
      text,
      threadId: randomUUID(),
    });

    res.status(200).json({ ok: true, result });
  });

  app.listen(4010, () => {
    console.info("[express] listening on http://127.0.0.1:4010");
  });
}

void bootstrap();
```
