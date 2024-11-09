from dotenv import load_dotenv; load_dotenv()
from os import getenv
from concurrent import futures
import grpc
from internal.proto import data_pb2_grpc
from services import InteractionsService


def serve():
    PORT = getenv('PORT', '50051')
    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))    
        data_pb2_grpc.add_InteractionsServiceServicer_to_server(InteractionsService(), server)
        server.add_insecure_port(f'[::]:{PORT}')
        server.start()
        print(f'Server listen on port {PORT}')
        server.wait_for_termination()
    except KeyboardInterrupt:
        print('\nStoping...')


if __name__ == '__main__':
    serve()
