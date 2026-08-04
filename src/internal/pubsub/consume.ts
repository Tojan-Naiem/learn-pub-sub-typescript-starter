import amqp, { type Channel, type ConfirmChannel } from "amqplib"
import { encode, decode } from "@msgpack/msgpack";
import type { GameLog } from "../gamelogic/logs.js";
import { publishMsgPack } from "./publish.js";
import { ExchangePerilTopic, GameLogSlug } from "../routing/routing.js";

export enum SimpleQueueType {
  Durable,
  Transient,
}
export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}
export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]>{

    const channel=await conn.createChannel();
    const queue =await channel.assertQueue(queueName,{
        durable:queueType===SimpleQueueType.Durable,
        autoDelete:queueType===SimpleQueueType.Transient,
        exclusive:queueType===SimpleQueueType.Transient,
        arguments:{
            "x-dead-letter-exchange":"peril_dlx"
        }
    })
    await channel.bindQueue(
        queueName,
        exchange,
        key
    )
    return [
        channel,
        queue
    ]
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType>| AckType,
): Promise<void>{
    const [channel,queue]=await declareAndBind(
        conn,
        exchange,
        queueName,
        key,
        queueType
    )
    await channel.prefetch(10);
    channel.consume(
        queue.queue,
        async (msg)=>{
            if(!msg)return;
            const data=JSON.parse(
                msg.content.toString()
            )
            const ackType = await handler(data)
            console.log(`Ack Type : ${ackType}`)
try{
switch(ackType) {
  case AckType.Ack:
    channel.ack(msg)
    break

  case AckType.NackRequeue:
    channel.nack(msg, false, true)
    break

  case AckType.NackDiscard:
    channel.nack(msg, false, false)
    break
}
} catch(err) {
    channel.nack(msg, false, false)
}

        }
    )

}


export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType>| AckType,
): Promise<void>{
    const [channel,queue]=await declareAndBind(
        conn,
        exchange,
        queueName,
        key,
        queueType
    )
    await channel.prefetch(10);
    channel.consume(
        queue.queue,
        async (msg)=>{
            if(!msg)return;
            const data=decode(
                msg.content
            ) as T;
            const ackType = await handler(data)
            console.log(`Ack Type : ${ackType}`)
try{
switch(ackType) {
  case AckType.Ack:
    channel.ack(msg)
    break

  case AckType.NackRequeue:
    channel.nack(msg, false, true)
    break

  case AckType.NackDiscard:
    channel.nack(msg, false, false)
    break
}
} catch(err) {
    channel.nack(msg, false, false)
}

        }
    )

}


export async function publishGameLog(ch:ConfirmChannel,username:string,msg:string){
  const gameLog:GameLog={
    username:username,
    message:msg,
    currentTime:new Date()
  }
  await publishMsgPack(
    ch,
    ExchangePerilTopic,
    `${GameLogSlug}.${username}`,
    gameLog
  )

}