import amqp, { type ConfirmChannel } from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import {
  AckType,
  declareAndBind,
  SimpleQueueType,
  subscribeJSON,
} from "../internal/pubsub/consume.js";
import {
  ArmyMovesPrefix,
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove, handleMove } from "../internal/gamelogic/move.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { handleWar } from "../internal/gamelogic/war.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const publishChannel = await conn.createConfirmChannel();
  const username = await clientWelcome();
  const gs = new GameState(username);

  const [channel, queue] = await declareAndBind(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
  );

  const [channelMove, queueMove] = await declareAndBind(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
  );
  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
    await handlerPause(gs),
  );
  await subscribeJSON<ArmyMove>(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    await handlerMove(gs, publishChannel),
  );
  // war
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `war`,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    await handlerWar(gs),
  );

  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }
    const command = words[0];
    switch (command) {
      case "spawn":
        await commandSpawn(gs, words);
        break;
      case "move":
        const armyMove = await commandMove(gs, words);
        await publishJSON(
          publishChannel,
          ExchangePerilTopic,
          `${ArmyMovesPrefix}.${username}`,
          armyMove,
        );
        console.log("Move published successfully");
        break;
      case "status":
        await commandStatus(gs);
        break;
      case "help":
        await printClientHelp();
        break;
      case "spam":
        console.log("Spamming not allowed yet!");
        break;
      case "quit":
        printQuit();
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
