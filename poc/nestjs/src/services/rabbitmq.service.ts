import { randomUUID } from "node:crypto";
import { Injectable, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { RabbitMQClient, RabbitMqBrokerRouter } from "@naskot/node-rabbitmq-brokers";
import { appConfig } from "../config.js";
import { createExpressToNestAction } from "../actions/expressToNest.action.js";
import { createNestToExpressAction, type PingPongPayload } from "../actions/nestToExpress.action.js";
import { HmacService } from "./hmac.service.js";

const MESSAGE_MARKER = "***MESSAGE_FROM_NASKOT***";

@Injectable()
export class RabbitMqService implements OnModuleInit, OnModuleDestroy {
  private client: RabbitMQClient | null = null;
  private router: RabbitMqBrokerRouter | null = null;
  private readonly hmacService: HmacService;
  private startupPingTimerRefs: NodeJS.Timeout[] = [];

  constructor(hmacService: HmacService) {
    this.hmacService = hmacService;
  }

  async onModuleInit() {
    const hmacService = this.hmacService;

    this.client = new RabbitMQClient("POC_NEST", appConfig.rabbitmq, {
      hmac: {
        signMessage: async (keyId, payload) => await hmacService.signMessage(keyId, payload),
        verifyMessage: async (message) => await hmacService.verifyMessage(message),
      },
      logger: {
        info: (meta, msg) => console.info(msg, meta),
        warn: (meta, msg) => console.warn(msg, meta),
        error: (meta, msg) => console.error(msg, meta),
        debug: (meta, msg) => console.info(msg, meta),
      },
    });

    const nestToExpressAction = createNestToExpressAction();
    const expressToNestAction = createExpressToNestAction(async (incoming) => {
      await this.publishToExpress({
        kind: "pong",
        text: `nest pong for ${incoming.threadId}`,
        threadId: incoming.threadId,
      });
    });

    this.router = new RabbitMqBrokerRouter({
      client: this.client,
      queue: appConfig.broker.queue,
      bindingKey: appConfig.broker.bindingKey,
      topicExchange: appConfig.broker.topicExchange,
      fanoutExchange: appConfig.broker.fanoutExchange,
      unknownTypeRetryDelayMs: appConfig.broker.unknownTypeRetryDelayMs,
      logger: {
        info: (meta, msg) => console.info(msg, meta),
        warn: (meta, msg) => console.warn(msg, meta),
        error: (meta, msg) => console.error(msg, meta),
        debug: (meta, msg) => console.info(msg, meta),
      },
    });

    this.router.registerAction(expressToNestAction);

    await this.router.startConsumer({
      allowedKeyIds: ["express_app"],
      prefetchCount: 5,
    });

    this.scheduleStartupPings();

    this.cachedNestToExpressAction = nestToExpressAction;
  }

  async onModuleDestroy() {
    for (const timerRef of this.startupPingTimerRefs) {
      clearTimeout(timerRef);
    }

    this.startupPingTimerRefs = [];

    if (this.client) {
      await this.client.close();
    }
  }

  private cachedNestToExpressAction: ReturnType<typeof createNestToExpressAction> | null = null;

  private scheduleStartupPings() {
    const startupDelaysMs = [2000, 6500];

    for (const delayMs of startupDelaysMs) {
      const timerRef = setTimeout(() => {
        void this.publishToExpress({
          kind: "ping",
          text: "nest startup ping",
          threadId: randomUUID(),
        });
      }, delayMs);

      this.startupPingTimerRefs.push(timerRef);
    }
  }

  async publishToExpress(input: { kind: "ping" | "pong"; text: string; threadId: string }) {
    if (!this.router || !this.cachedNestToExpressAction) {
      throw new Error("rabbitmq router is not ready");
    }

    const taggedText = input.text.includes(MESSAGE_MARKER) ? input.text : `${MESSAGE_MARKER} ${input.text}`;

    console.info("[nestjs] sending to express", {
      marker: MESSAGE_MARKER,
      kind: input.kind,
      threadId: input.threadId,
      text: taggedText,
    });

    const payload: PingPongPayload = {
      from: "nest",
      kind: input.kind,
      text: taggedText,
      threadId: input.threadId,
      at: Date.now(),
    };

    return await this.router.publish(
      "nest_app",
      this.cachedNestToExpressAction,
      {
        message: `${MESSAGE_MARKER} [nest] ${input.kind}`,
        payload,
      },
      [appConfig.broker.publishRoutingKeyToExpress]
    );
  }
}
