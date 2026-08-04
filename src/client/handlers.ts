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
import { AckType, publishGameLog, SimpleQueueType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

export function handlerPause(
  gs: GameState,
): (ps: PlayingState) => Promise<AckType> {
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

export function handlerMove(
  gs: GameState,
  ch: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
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
      try {
        await publishJSON(
          ch,
          ExchangePerilTopic,
          `${WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`,
          rw,
        );
        return AckType.Ack;
      } catch (err) {
        return AckType.NackRequeue;
      }
    }

    return AckType.Ack;
  };
}

export function handlerWar(
  gs: GameState,
  ch: ConfirmChannel,
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar): Promise<AckType> => {
    const outcome = handleWar(gs, rw);
    if (outcome.result === WarOutcome.NotInvolved) {
      process.stdout.write("> ");
      return AckType.NackRequeue;
    } else if (outcome.result === WarOutcome.NoUnits) {
      process.stdout.write("> ");
      return AckType.NackDiscard;
    }
    let message: string;

    if (
      outcome.result === WarOutcome.OpponentWon ||
      outcome.result === WarOutcome.YouWon
    ) {
      message = `${outcome.winner} won a war against ${outcome.loser}`;
    } else if (outcome.result === WarOutcome.Draw) {
      message = `A war between ${rw.attacker.username} and ${rw.defender.username} resulted in a draw`;
    } else {
      process.stdout.write("> ");
      return AckType.NackDiscard;
    }

    const gameLog: GameLog = {
      username: rw.attacker.username,
      message,
      currentTime: new Date(),
    };

    try {
      await publishGameLog(ch, gs.getPlayerSnap().username, message);
      process.stdout.write("> ");
      return AckType.Ack;
    } catch (err) {
      console.error(err);
      process.stdout.write("> ");
      return AckType.NackRequeue;
    }
  };
}



