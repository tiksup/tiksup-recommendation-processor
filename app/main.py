from internal.proto import user_pb2_grpc
from services.grpc import InteractionsService
from services.recommendations import Affinity
from services.recommendations import Recommender
from services.queue import Queue
from internal.config import RedisConnection
from concurrent import futures
from dotenv import load_dotenv
from os import getenv
import grpc


load_dotenv()
PORT = getenv('PORT', '50051')

def serve():
    try:
        conn = RedisConnection()
        rdb = conn.connect()

        affinity = Affinity(Recommender())
        affinity.create_session()
        queue = Queue(rdb)

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        user_pb2_grpc.add_InteractionsServiceServicer_to_server(InteractionsService(
            affinity,
            queue
        ), server)
        server.add_insecure_port(f'[::]:{PORT}')

        server.start()
        print(f'Server listen on port {PORT}')
        server.wait_for_termination()
    except KeyboardInterrupt:
        print('\nStoping...')


if __name__ == '__main__':
    serve()
