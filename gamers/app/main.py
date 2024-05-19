from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
import asyncio
from contextlib import asynccontextmanager

import logging

logging.basicConfig(level=logging.INFO)

KAFKA_BROKER = "broker:19092"
KAFKA_TOPIC = "gamers"
KAFKA_CONSUMER_GROUP_ID = "gamers-consumer-group"

async def consume():
    # Milestone: CONSUMER INTIALIZE
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER
    )
    
    await consumer.start()
    try:
        async for msg in consumer:
            logging.info(
                "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp)
            )
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting consumer")
    asyncio.create_task(consume())
    yield
    print("Stopping consumer")

app = FastAPI(lifespan=lifespan)


@app.get("/")
def hello():
    return {"Hello": "World"}
