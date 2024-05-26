from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
from contextlib import asynccontextmanager
from sqlmodel import SQLModel
import logging

from app import gamers_pb2

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
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest"

    )

    await consumer.start()
    try:
        async for msg in consumer:
            logging.info("RAW MESSAGE: %s", msg.value)
            deserialize_message = gamers_pb2.GamePlayers()
            deserialize_message.ParseFromString(msg.value)
            logging.info("DERSERLIZSEA: %s", deserialize_message)

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

    player_data_prot = gamers_pb2.GamePlayers(
        player_name=player_data.player_name, age=player_data.age, email=player_data.email, phone_number=player_data.phone_number)
    print("player_data_prot", player_data_prot)

    player_data_prot_serialized = player_data_prot.SerializeToString()
    print("player_data_prot_serialized", player_data_prot_serialized)

    await producer.start()

    try:
        await producer.send_and_wait(KAFKA_TOPIC, player_data_prot_serialized)
    finally:
        await producer.stop()

    return player_data.model_dump_json()
