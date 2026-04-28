import { describe, expect, it, vi } from "vitest";
import { RabbitMqBrokerRouter, brokerRouterHelpers } from "../src/index.js";
import type {
  BrokerPayload,
  ExchangeOptions,
  ExchangeType,
  MessageContext,
  QueueOptions,
  RabbitMQConsumerOptions,
  RabbitMQProducerOptions,
  RabbitMqBrokerActionContract,
  RabbitMqBrokerRouterOptions,
} from "../src/index.js";

const createClientHarness = () => {
  type RouterClient = RabbitMqBrokerRouterOptions["client"];
  const publishResults: boolean[] = [];
  const stats = {
    connectCount: 0,
    assertExchangeCalls: [] as Array<{ exchange: string; type: ExchangeType; options?: ExchangeOptions }>,
    assertQueueCalls: [] as Array<{ queue: string; options?: QueueOptions }>,
    bindQueueCalls: [] as Array<{ queue: string; exchange: string; routingKey: string }>,
    publishCalls: [] as Array<{ options: RabbitMQProducerOptions; payload: unknown }>,
    consumeCalls: [] as Array<{ options: RabbitMQConsumerOptions }>,
  };

  let consumeHandler: ((payload: BrokerPayload, message: MessageContext) => Promise<void> | void) | null = null;

  const client: RouterClient = {
    connect: () => {
      stats.connectCount += 1;
      return Promise.resolve();
    },
    assertExchange: (exchange: string, type: ExchangeType, options?: ExchangeOptions) => {
      stats.assertExchangeCalls.push({ exchange, type, options });
      return Promise.resolve();
    },
    assertQueue: (queue: string, options?: QueueOptions) => {
      stats.assertQueueCalls.push({ queue, options });
      return Promise.resolve({});
    },
    bindQueue: (queue: string, exchange: string, routingKey: string) => {
      stats.bindQueueCalls.push({ queue, exchange, routingKey });
      return Promise.resolve();
    },
    publish: <TPayload>(options: RabbitMQProducerOptions, payload: TPayload) => {
      stats.publishCalls.push({ options, payload });
      const next = publishResults.length > 0 ? publishResults.shift() : true;
      return Promise.resolve(next ?? true);
    },
    consume: (options, handler) => {
      stats.consumeCalls.push({ options });
      consumeHandler = handler as unknown as (payload: BrokerPayload, message: MessageContext) => Promise<void> | void;
      return Promise.resolve({});
    },
  };

  const getConsumeHandler = () => {
    if (!consumeHandler) {
      throw new Error("consume handler not initialized");
    }

    return consumeHandler;
  };

  return {
    client,
    stats,
    setPublishResults: (...results: boolean[]) => {
      publishResults.splice(0, publishResults.length, ...results);
    },
    getConsumeHandler,
  };
};

const createRouterHarness = () => {
  const clientHarness = createClientHarness();

  const router = new RabbitMqBrokerRouter({
    client: clientHarness.client,
    queue: "test.queue",
    bindingKey: "test.#",
    topicExchange: "test.topic",
    fanoutExchange: "test.fanout",
    unknownTypeRetryDelayMs: 4321,
  });

  return {
    router,
    ...clientHarness,
  };
};

const createAction = (
  type: string,
  consumeImpl?: (message: BrokerPayload, payload: Record<string, unknown> | null) => Promise<void> | void
) => {
  const consume = vi.fn(consumeImpl ?? (() => {}));

  const action: RabbitMqBrokerActionContract<Record<string, unknown>> = {
    type,
    publish: (input) => ({
      message: input.message,
      data: JSON.stringify(input.payload),
      timestamp: Date.now(),
    }),
    consume,
  };

  return {
    action,
    consume,
  };
};

const createMessageContext = () => {
  const ack = vi.fn();
  const nack = vi.fn();
  const reject = vi.fn();
  const defer = vi.fn(() => Promise.resolve());

  const message: MessageContext = {
    fields: {
      consumerTag: "ctag-1",
      deliveryTag: 1,
      redelivered: false,
      exchange: "test.topic",
      routingKey: "test.route",
    },
    properties: {
      contentType: "application/json",
    },
    content: Buffer.from("{}", "utf-8"),
    ack,
    nack,
    reject,
    defer,
  };

  return {
    message,
    ack,
    nack,
    reject,
    defer,
  };
};

describe("brokerRouterHelpers.normalizeActionType", () => {
  it("normalizes action file paths", () => {
    const output = brokerRouterHelpers.normalizeActionType("./src/brokers/actions/test/InstructionTestAction.ts");
    expect(output).toBe("test/InstructionTestAction");
  });

  it("normalizes windows paths", () => {
    const output = brokerRouterHelpers.normalizeActionType(".\\src\\actions\\orders\\CreateOrderAction.js");
    expect(output).toBe("orders/CreateOrderAction");
  });

  it("rejects unsafe values", () => {
    const output = brokerRouterHelpers.normalizeActionType("../../etc/passwd");
    expect(output).toBeNull();
  });

  it("rejects invalid chars", () => {
    const output = brokerRouterHelpers.normalizeActionType("orders/create$order");
    expect(output).toBeNull();
  });
});

describe("brokerRouterHelpers.parsePayloadData", () => {
  it("parses valid json object", () => {
    const output = brokerRouterHelpers.parsePayloadData('{"ok":true,"count":1}');
    expect(output).toEqual({ ok: true, count: 1 });
  });

  it("returns null for invalid json", () => {
    const output = brokerRouterHelpers.parsePayloadData("{not-json}");
    expect(output).toBeNull();
  });

  it("returns null for primitive json", () => {
    const output = brokerRouterHelpers.parsePayloadData("42");
    expect(output).toBeNull();
  });
});

describe("RabbitMqBrokerRouter", () => {
  it("throws on invalid registerAction type", () => {
    const { router } = createRouterHarness();

    const register = () => {
      router.registerAction({
        type: "../../bad",
        publish: () => ({ message: "bad", data: "{}", timestamp: Date.now() }),
        consume: () => {},
      });
    };

    expect(register).toThrow("invalid action.type");
  });

  it("validates publish input before touching topology", async () => {
    const { router, stats } = createRouterHarness();
    const { action } = createAction("orders/create");

    const output = await router.publish("express_app", action, {
      message: "",
      payload: { ok: true },
    });

    expect(output).toEqual({
      published: false,
      err: { status: 400, code: "bad_payload_message", message: "payload.message is required" },
    });
    expect(stats.connectCount).toBe(0);
  });

  it("publishes to fanout when no routing keys are provided", async () => {
    const { router, stats } = createRouterHarness();
    const { action } = createAction("orders/create");

    const output = await router.publish(
      "express_app",
      action,
      {
        message: "hello",
        payload: { orderId: 1 },
      },
      []
    );

    expect(output).toEqual({ published: true });
    expect(stats.publishCalls).toHaveLength(1);

    const firstCall = stats.publishCalls[0];
    expect(firstCall?.options).toMatchObject({
      exchange: "test.fanout",
      routingKey: "",
      hmacKeyId: "express_app",
    });
    expect(firstCall?.payload).toMatchObject({
      type: "orders/create",
      message: "hello",
    });
  });

  it("publishes to topic for sanitized routing keys", async () => {
    const { router, stats } = createRouterHarness();
    const { action } = createAction("orders/create");

    const output = await router.publish(
      "express_app",
      action,
      {
        message: "hello",
        payload: { orderId: 2 },
      },
      ["  ", " orders.created ", "orders.audit"]
    );

    expect(output).toEqual({ published: true });
    expect(stats.publishCalls).toHaveLength(2);

    const firstCall = stats.publishCalls[0];
    const secondCall = stats.publishCalls[1];

    expect(firstCall?.options).toMatchObject({
      exchange: "test.topic",
      routingKey: "orders.created",
      hmacKeyId: "express_app",
    });
    expect(secondCall?.options).toMatchObject({
      exchange: "test.topic",
      routingKey: "orders.audit",
      hmacKeyId: "express_app",
    });
  });

  it("returns published false if one topic publish fails", async () => {
    const { router, setPublishResults } = createRouterHarness();
    const { action } = createAction("orders/create");

    setPublishResults(true, false);

    const output = await router.publish(
      "express_app",
      action,
      {
        message: "hello",
        payload: { orderId: 3 },
      },
      ["orders.created", "orders.audit"]
    );

    expect(output).toEqual({ published: false });
  });

  it("routes consumed payload to matching action and acks on success", async () => {
    const { router, stats, getConsumeHandler } = createRouterHarness();
    const { action, consume: consumeSpy } = createAction("orders/create");

    router.registerAction(action);
    await router.startConsumer({ allowedKeyIds: ["express_app"], prefetchCount: 8 });

    expect(stats.consumeCalls).toHaveLength(1);
    const consumeCall = stats.consumeCalls[0];
    expect(consumeCall?.options).toEqual({
      queue: "test.queue",
      allowedKeyIds: ["express_app"],
      prefetchCount: 8,
      autoAck: false,
    });

    const handler = getConsumeHandler();
    const { message, ack, nack, defer } = createMessageContext();

    await handler(
      {
        type: "./src/actions/orders/create.ts",
        data: '{"orderId":42}',
      },
      message
    );

    expect(consumeSpy).toHaveBeenCalledTimes(1);
    const consumeSpyCall = consumeSpy.mock.calls[0];
    expect(consumeSpyCall?.[1]).toEqual({ orderId: 42 });

    expect(ack).toHaveBeenCalledTimes(1);
    expect(nack).not.toHaveBeenCalled();
    expect(defer).not.toHaveBeenCalled();
  });

  it("defers unknown action types", async () => {
    const { router, getConsumeHandler } = createRouterHarness();

    await router.startConsumer();

    const handler = getConsumeHandler();
    const { message, ack, nack, defer } = createMessageContext();

    await handler(
      {
        type: "unknown/action",
        data: '{"hello":"world"}',
      },
      message
    );

    expect(defer).toHaveBeenCalledTimes(1);
    expect(defer).toHaveBeenCalledWith(4321);
    expect(ack).not.toHaveBeenCalled();
    expect(nack).not.toHaveBeenCalled();
  });

  it("nacks with requeue when action handler throws", async () => {
    const { router, getConsumeHandler } = createRouterHarness();
    const { action } = createAction("orders/create", () => {
      throw new Error("boom");
    });

    router.registerAction(action);
    await router.startConsumer();

    const handler = getConsumeHandler();
    const { message, ack, nack, defer } = createMessageContext();

    await handler(
      {
        type: "orders/create",
        data: '{"orderId":7}',
      },
      message
    );

    expect(nack).toHaveBeenCalledTimes(1);
    expect(nack).toHaveBeenCalledWith(false, true);
    expect(ack).not.toHaveBeenCalled();
    expect(defer).not.toHaveBeenCalled();
  });

  it("starts consumer only once", async () => {
    const { router, stats } = createRouterHarness();

    await router.startConsumer();
    await router.startConsumer();

    expect(stats.consumeCalls).toHaveLength(1);
    expect(stats.assertExchangeCalls).toHaveLength(2);
    expect(stats.assertQueueCalls).toHaveLength(1);
    expect(stats.bindQueueCalls).toHaveLength(2);
  });
});
