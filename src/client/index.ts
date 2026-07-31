import amqp, { type ConfirmChannel } from "amqplib"
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerPause } from "./handlers.js";


async function main() {
  console.log("Starting Peril client...");
   const rabbitConnString="amqp://guest:guest@localhost:5672/"
    const conn=await amqp.connect(rabbitConnString)
    const username=await clientWelcome()
    const gs = new GameState(username);

    const [channel,queue]=await declareAndBind(
      conn,
      ExchangePerilDirect,
      `${PauseKey}.${username}`,
      PauseKey,
      SimpleQueueType.Transient

    )
await subscribeJSON(
  conn,
  ExchangePerilDirect,
  `${PauseKey}.${username}`,
  PauseKey,
  SimpleQueueType.Transient,
  handlerPause(gs)
);
    while(true){
      const words=await getInput()
      if(words.length===0){
        continue;
      }
      const command=words[0]
      switch(command){
        case "spawn":
          await commandSpawn(gs,words)
          break
        case "move":
          await commandMove(gs,words)
          break;
        case "status":
          await commandStatus(gs)
          break;
        case "help":
          await printClientHelp();
          break;
        case "spam":
          console.log("Spamming not allowed yet!")
          break;
        case "quit":
          printQuit()
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
