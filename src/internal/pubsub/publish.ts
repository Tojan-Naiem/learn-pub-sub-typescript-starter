import type { ConfirmChannel } from "amqplib";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void>{
    return new Promise((resolve,reject)=>{
        const buffer=Buffer.from(JSON.stringify(value));
        ch.publish(
            exchange,
            routingKey,
            buffer,
            {
                contentType:"application/json"
            },
            (err)=>{
                if(err){
                    reject(err)
                }else{
                    resolve()
                }
            }
        )
    })
}