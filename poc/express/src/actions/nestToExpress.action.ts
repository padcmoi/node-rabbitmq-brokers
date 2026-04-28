import type { BrokerActionPublishInput, BrokerPayload, RabbitMqBrokerActionContract } from "@naskot/node-rabbitmq-brokers";
import type { PingPongPayload } from "./expressToNest.action.js";

export function createNestToExpressAction(onPing?: (payload: PingPongPayload) => Promise<void>) {
  const action: RabbitMqBrokerActionContract<PingPongPayload> = {
    type: "nest/hello",
    publish: (input: BrokerActionPublishInput<PingPongPayload>) => {
      return {
        message: input.message,
        data: JSON.stringify(input.payload),
        timestamp: Date.now(),
      };
    },
    consume: async (message: BrokerPayload, payload: PingPongPayload | null) => {
      console.info("[express] received from nest", {
        envelope: message,
        payload,
      });

      if (payload?.kind === "ping" && onPing) {
        await onPing(payload);
      }
    },
  };

  return action;
}
