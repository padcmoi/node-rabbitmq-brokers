# NestJS Integration

This guide shows how to integrate `@naskot/node-rabbitmq-brokers` in a NestJS application with providers/services.

## 1) Install

```bash
npm install @naskot/node-rabbitmq-brokers @naskot/node-hmac-auth amqplib @nestjs/common @nestjs/core @nestjs/platform-express redis reflect-metadata rxjs
```

## 2) Complete Service Setup

Recommended split:

- `src/services/hmac.service.ts`
- `src/services/rabbitmq.service.ts`
- `src/actions/*.action.ts`
- `src/app.module.ts`

### `src/services/hmac.service.ts`

```ts
import { randomUUID } from "node:crypto";
import { Injectable, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { createClient, type RedisClientType } from "redis";
import { initializeHmacMessageAuth, type InitializedHmacMessageAuth } from "@naskot/node-hmac-auth";
import type { SignedHmacMessage } from "@naskot/node-rabbitmq-brokers";

@Injectable()
export class HmacService implements OnModuleInit, OnModuleDestroy {
  private redisClient: RedisClientType | null = null;
  private hmacMessageAuth: InitializedHmacMessageAuth | null = null;

  async onModuleInit() {
    this.redisClient = createClient({ url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379" });
    await this.redisClient.connect();

    this.hmacMessageAuth = initializeHmacMessageAuth({
      redis: this.redisClient,
      namespace: process.env.HMAC_NAMESPACE ?? "my-api-messages",
      secretToken: process.env.HMAC_SECRET_TOKEN,
    });

    const defaults = [
      { clientId: "express_app", secret: "express_secret" },
      { clientId: "nest_app", secret: "nest_secret" },
    ];

    for (const credential of defaults) {
      const existing = await this.hmacMessageAuth.clients.get(credential.clientId);
      if (existing) continue;

      await this.hmacMessageAuth.clients.create({
        clientId: credential.clientId,
        plainSecret: credential.secret,
      });
    }
  }

  async onModuleDestroy() {
    if (this.redisClient?.isOpen) {
      await this.redisClient.quit();
    }
  }

  private getInstance() {
    if (!this.hmacMessageAuth) {
      throw new Error("hmac message auth is not initialized");
    }

    return this.hmacMessageAuth;
  }

  async signMessage(clientId: string, payload: unknown) {
    const signed = await this.getInstance().signMessage({ clientId, message: payload });

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

  async verifyMessage(message: SignedHmacMessage) {
    try {
      await this.getInstance().verifyMessage({
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
}
```

### `src/services/rabbitmq.service.ts`

```ts
import { Injectable, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { randomUUID } from "node:crypto";
import { RabbitMQClient, RabbitMqBrokerRouter } from "@naskot/node-rabbitmq-brokers";
import { HmacService } from "./hmac.service.js";
import { createInboundAction } from "../actions/inbound.action.js";
import { createOutboundAction, type PingPayload } from "../actions/outbound.action.js";

@Injectable()
export class RabbitMqService implements OnModuleInit, OnModuleDestroy {
  private client: RabbitMQClient | null = null;
  private router: RabbitMqBrokerRouter | null = null;
  private outboundAction = createOutboundAction();

  constructor(private readonly hmacService: HmacService) {}

  async onModuleInit() {
    this.client = new RabbitMQClient(
      "MY_NEST_APP",
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
          signMessage: async (keyId, payload) => await this.hmacService.signMessage(keyId, payload),
          verifyMessage: async (message) => await this.hmacService.verifyMessage(message),
        },
      }
    );

    this.router = new RabbitMqBrokerRouter({
      client: this.client,
      queue: "my.nest.queue",
      bindingKey: "my.nest.#",
      topicExchange: "my.bus.topic",
      fanoutExchange: "my.bus.fanout",
      unknownTypeRetryDelayMs: 3000,
    });

    const inboundAction = createInboundAction(async (payload) => {
      await this.publishToExpress({
        kind: "pong",
        text: `pong for ${payload.threadId}`,
        threadId: payload.threadId,
      });
    });

    this.router.registerAction(inboundAction);

    await this.router.startConsumer({
      allowedKeyIds: ["express_app"],
      prefetchCount: 5,
    });

    await this.publishToExpress({
      kind: "ping",
      text: "nest startup ping",
      threadId: randomUUID(),
    });
  }

  async onModuleDestroy() {
    if (this.client) {
      await this.client.close();
    }
  }

  async publishToExpress(input: { kind: "ping" | "pong"; text: string; threadId: string }) {
    if (!this.router) {
      throw new Error("rabbitmq router is not ready");
    }

    const payload: PingPayload = {
      from: "nest",
      kind: input.kind,
      text: input.text,
      threadId: input.threadId,
      at: Date.now(),
    };

    return await this.router.publish(
      "nest_app",
      this.outboundAction,
      {
        message: `[nest] ${input.kind}`,
        payload,
      },
      ["my.express.events"]
    );
  }
}
```

## 3) Helpers and Responsibilities

Recommended helper responsibilities:

- `HmacService.getInstance()`: defensive accessor for initialized HMAC runtime
- `HmacService.signMessage()`: Nest adapter for broker signing contract
- `HmacService.verifyMessage()`: Nest adapter for broker verification contract
- `RabbitMqService.publishToExpress()`: single publish entrypoint for business code

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
  app.controller.ts
  app.module.ts
  main.ts
```

Action contract rules:

- each action must define a stable `type`
- `publish(...)` maps business payload -> broker payload
- `consume(...)` handles incoming message + parsed payload
- keep transport logic in actions, orchestration in services/providers

## 5) NestJS App Usage

### `src/app.controller.ts`

```ts
import { Body, Controller, Get, Post } from "@nestjs/common";
import { randomUUID } from "node:crypto";
import { RabbitMqService } from "./services/rabbitmq.service.js";

@Controller()
export class AppController {
  constructor(private readonly rabbitMqService: RabbitMqService) {}

  @Get("health")
  health() {
    return { ok: true, app: "nestjs" };
  }

  @Post("publish/to-express")
  async publishToExpress(@Body() body: { text?: string }) {
    const text = typeof body?.text === "string" && body.text.trim() ? body.text.trim() : "ping from nest";

    const result = await this.rabbitMqService.publishToExpress({
      kind: "ping",
      text,
      threadId: randomUUID(),
    });

    return { ok: true, result };
  }
}
```

### `src/app.module.ts`

```ts
import { Module } from "@nestjs/common";
import { AppController } from "./app.controller.js";
import { HmacService } from "./services/hmac.service.js";
import { RabbitMqService } from "./services/rabbitmq.service.js";

@Module({
  controllers: [AppController],
  providers: [HmacService, RabbitMqService],
})
export class AppModule {}
```

### `src/main.ts`

```ts
import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module.js";

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  await app.listen(4020);
  console.info("[nestjs] listening on http://127.0.0.1:4020");
}

void bootstrap();
```
