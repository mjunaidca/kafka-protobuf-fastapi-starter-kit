from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
from contextlib import asynccontextmanager
from sqlmodel import SQLModel
import logging

logging.basicConfig(level=logging.INFO)

KAFKA_BROKER = "broker:19092"
KAFKA_TOPIC = "gamers"
KAFKA_CONSUMER_GROUP_ID = "gamers-consumer-group"

class GamePlayersRegistration(SQLModel):
    player_name: str
    age: int
    email: str
    phone_number: str


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


@app.post("/register-player")
async def register_new_player(player_data: GamePlayersRegistration):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)

    await producer.start()

    try:
        await producer.send_and_wait(KAFKA_TOPIC, player_data.model_dump_json().encode('utf-8'))
    finally:
        await producer.stop()

    return player_data.model_dump_json() 
