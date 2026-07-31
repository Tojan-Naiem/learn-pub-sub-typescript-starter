import amqp, { type ConfirmChannel } from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);

  const channel: ConfirmChannel = await conn.createConfirmChannel();
  await channel.assertExchange(ExchangePerilDirect, "direct", {
    durable: true,
  });

  await channel.assertExchange(
    ExchangePerilTopic,"topic",{
      durable:true
    }
  )
      await declareAndBind(
      conn,
      ExchangePerilTopic,
      GameLogSlug,
      `${GameLogSlug}.`,
      SimpleQueueType.Durable
    )
  console.log("Connected to RabbitMQ");
  process.on("SIGINT", async () => {
    console.log("Shutting down...");
    await conn.close();
    process.exit(0);
  });
  printServerHelp();
  while (true) {
    const words = await getInput();
    if (words.length === 0) continue;
    const command = words[0];
    switch (command) {
      case "pause":
        await publishJSON(channel, ExchangePerilDirect, PauseKey, {
          isPaused: true,
        });
        break;

      case "resume":
        await publishJSON(channel, ExchangePerilDirect, PauseKey, {
          isPaused: false,
        });
        break;

      case "quit":
        console.log("Exiting...");
        return;

      default:
        console.log("I don't understand that command.");
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
