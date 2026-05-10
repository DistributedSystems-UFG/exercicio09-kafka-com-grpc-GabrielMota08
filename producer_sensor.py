from kafka import KafkaProducer
from const import *
import random
import time

producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]
)

while True:
    temperatura = round(random.uniform(20, 40), 2)

    print(f'Enviando temperatura: {temperatura}°C')

    producer.send(
        TOPIC_RAW,
        value=str(temperatura).encode()
    )

    producer.flush()

    time.sleep(3)