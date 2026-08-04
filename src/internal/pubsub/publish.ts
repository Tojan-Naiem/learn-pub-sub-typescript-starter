import type { ConfirmChannel } from "amqplib";
import { encode, decode } from "@msgpack/msgpack";
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

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void>{
    return new Promise((resolve,reject)=>{
        const buffer=Buffer.from(encode(value));
        ch.publish(
            exchange,
            routingKey,
            buffer,
            {
                contentType:"application/x-msgpack"
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
