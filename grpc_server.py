from concurrent import futures
import grpc
from kafka import KafkaConsumer
from const import *

import temperatura_pb2
import temperatura_pb2_grpc

from database import salvar, listar

import threading

class TemperaturaService(
    temperatura_pb2_grpc.TemperaturaServiceServicer
):

    def ListarTemperaturas(self, request, context):

        dados = listar()

        resposta = []

        for d in dados:
            resposta.append(
                temperatura_pb2.Temperatura(
                    id=d[0],
                    temperatura=d[1],
                    media=d[2]
                )
            )

        return temperatura_pb2.TemperaturasResponse(
            temperaturas=resposta
        )

def consumir_kafka():

    consumer = KafkaConsumer(
        TOPIC_PROCESSADA,
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        auto_offset_reset='earliest'
    )

    print('Consumer final iniciado')

    for msg in consumer:

        texto = msg.value.decode()

        temperatura, media = texto.split(',')

        print(f'Salvando: {temperatura}°C / {media}°C')

        salvar(
            float(temperatura),
            float(media)
        )

def serve():

    threading.Thread(
        target=consumir_kafka,
        daemon=True
    ).start()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    temperatura_pb2_grpc.add_TemperaturaServiceServicer_to_server(
        TemperaturaService(),
        server
    )

    server.add_insecure_port('[::]:50051')

    server.start()

    print('Servidor gRPC iniciado na porta 50051')

    server.wait_for_termination()

serve()