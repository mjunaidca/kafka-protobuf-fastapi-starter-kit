from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
import asyncio
from contextlib import asynccontextmanager

KAFKA_BROKER = "broker:19092"
KAFKA_TOPIC = "gamers"
KAFKA_CONSUMER_GROUP_ID = "gamers-consumer-group"


async def consume():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_CONSUMER_GROUP_ID)
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("startup")
    asyncio.create_task(consume())
    print("Created Task")
    yield


app = FastAPI(lifespan=lifespan)



@app.get("/")
def hello():
    return {"Hello": "World"}