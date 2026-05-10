from kafka import KafkaConsumer, KafkaProducer
from const import *

consumer = KafkaConsumer(
    TOPIC_RAW,
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    auto_offset_reset='earliest'
)

producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]
)

historico = []

print('Processor iniciado')

for msg in consumer:

    temperatura = float(msg.value.decode())

    historico.append(temperatura)

    media = sum(historico) / len(historico)

    resultado = f'{temperatura},{round(media,2)}'

    print(f'Temp: {temperatura:.2f}°C | Média: {media:.2f}°C')
    print('---')

    producer.send(
        TOPIC_PROCESSADA,
        value=resultado.encode()
    )

    producer.flush()