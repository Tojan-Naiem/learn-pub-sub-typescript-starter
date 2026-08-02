import type { ConfirmChannel } from "amqplib";
import type {
  ArmyMove,
  RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import type {
  GameState,
  PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType, SimpleQueueType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";

export  function handlerPause(gs: GameState): (ps: PlayingState) =>Promise<AckType>{
  return async (ps: PlayingState) => {
    try {
      handlePause(gs, ps);
      process.stdout.write(">");
      return AckType.Ack;
    } catch (err) {
      console.error(err);
      return AckType.NackDiscard;
    }
  };
}

export  function handlerMove(gs: GameState, ch: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove): Promise<AckType> => {
    const outcome = handleMove(gs, move);

    if (outcome === MoveOutcome.SamePlayer) {
      return AckType.NackDiscard;
    }
    const rw: RecognitionOfWar = {
      attacker: move.player,
      defender: gs.getPlayerSnap(),
    };
    if (outcome == MoveOutcome.MakeWar) {
        try{
 await publishJSON(
        ch,
        ExchangePerilTopic,
        `${WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`,
        rw,
      );
            return AckType.Ack

        }
        catch(err){
                        return AckType.NackRequeue

        }
     
    }

    return AckType.Ack;
  };
}


export function handlerWar(gs:GameState): (rw: RecognitionOfWar)=>Promise<AckType>{
    return async (rw:RecognitionOfWar):Promise<AckType>=>{
        const outcome=handleWar(gs,rw);
        if(outcome.result===WarOutcome.NotInvolved){
            process.stdout.write("> ")
            return AckType.NackRequeue
        }
        else if(outcome.result===WarOutcome.NoUnits){
            process.stdout.write("> ")
            return AckType.NackDiscard;
        }
         else if(outcome.result===WarOutcome.OpponentWon){
            process.stdout.write("> ")
            return AckType.Ack;
        }
         else if(outcome.result===WarOutcome.YouWon){
            process.stdout.write("> ")
            return AckType.Ack;
        }
        else{
            console.log("Error Occured  ")
            process.stdout.write("> ")
            return AckType.NackDiscard
        }


    };


}