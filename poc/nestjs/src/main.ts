import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module.js";

async function bootstrap() {
  await NestFactory.createApplicationContext(AppModule, {
    logger: ["log", "error", "warn"],
  });

  console.info("[nestjs] worker mode enabled (no HTTP port)");
}

void bootstrap().catch((error) => {
  console.error("[nestjs] bootstrap failed", error);
  process.exit(1);
});
