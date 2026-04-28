import { Module } from "@nestjs/common";
import { HmacService } from "./services/hmac.service.js";
import { RabbitMqService } from "./services/rabbitmq.service.js";

@Module({
  imports: [],
  controllers: [],
  providers: [HmacService, RabbitMqService],
})
export class AppModule {}
