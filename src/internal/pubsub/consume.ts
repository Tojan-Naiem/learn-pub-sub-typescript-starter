import amqp, { type Channel, type ConfirmChannel } from "amqplib"

export enum SimpleQueueType {
  Durable,
  Transient,
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
        exclusive:queueType===SimpleQueueType.Transient
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
  handler: (data: T) => void,
): Promise<void>{
    const [channel,queue]=await declareAndBind(
        conn,
        exchange,
        queueName,
        key,
        queueType
    )
    channel.consume(
        queue.queue,
        (msg)=>{
            if(!msg)return;
            const data=JSON.parse(
                msg.content.toString()
            )
            handler(data)
            channel.ack(msg)

        }
    )

}