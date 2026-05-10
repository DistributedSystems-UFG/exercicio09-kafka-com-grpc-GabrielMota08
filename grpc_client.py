import grpc

import temperatura_pb2
import temperatura_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')

stub = temperatura_pb2_grpc.TemperaturaServiceStub(channel)

response = stub.ListarTemperaturas(
    temperatura_pb2.Empty()
)

print('\nÚLTIMAS TEMPERATURAS:\n')

for t in response.temperaturas:
    print(
        f'IDENTIFICADOR: {t.id} | Temp: {t.temperatura:.2f}°C | Média: {t.media:.2f}°C'
    )