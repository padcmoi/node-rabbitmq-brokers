import { bootstrapHmacService } from "./services/hmac.service.js";
import { bootstrapRabbitMqService } from "./services/rabbitmq.service.js";

async function waitForRabbitReady() {
  for (;;) {
    try {
      await bootstrapRabbitMqService();
      return;
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown_error";
      console.warn("[express] rabbit bootstrap failed, retry in 2s", { message });
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
}

async function bootstrap() {
  await bootstrapHmacService();
  await waitForRabbitReady();
  console.info("[express] worker mode enabled (no HTTP port)");
}

void bootstrap().catch((error) => {
  console.error("[express] bootstrap failed", error);
  process.exit(1);
});
