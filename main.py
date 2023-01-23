import asyncio
import json
import uvicorn
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI

from pydantic import BaseModel, StrictStr


class ProducerResponse(BaseModel):
    name: StrictStr
    message_id: StrictStr
    topic: StrictStr
    timestamp: StrictStr = ""
    lastName: StrictStr


class ProducerMessage(BaseModel):
    name: StrictStr
    message_id: StrictStr = ""
    timestamp: StrictStr = ""
    lastName: StrictStr
app = FastAPI()

KAFKA_INSTANCE = "localhost:9092"


loop = asyncio.get_event_loop()


aioproducer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_INSTANCE)

consumer = AIOKafkaConsumer("test1", bootstrap_servers=KAFKA_INSTANCE, loop=loop)


async def consume():
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                "consumed: ",
                msg.topic,
                msg.partition,
                msg.offset,
                msg.key,
                msg.value,
                msg.timestamp,
                msg.lastName,
            )

    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event():
    await aioproducer.start()
    loop.create_task(consume())


@app.on_event("shutdown")
async def shutdown_event():
    await aioproducer.stop()
    await consumer.stop()


@app.get("/")
def read_root():
    return {"Ping": "Pong"}

@app.post("/producer/{topicname}")
async def kafka_produce(msg: ProducerMessage, topicname: str):

    await aioproducer.send(topicname, json.dumps(msg.dict()).encode("ascii"))
    response = ProducerResponse(
        name=msg.name, message_id=msg.message_id, topic=topicname , lastName= msg.lastName
    )

    ##await aioproducer.stop()

    return response

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get('PORT', 8004)), log_level="info")