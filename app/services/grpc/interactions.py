from google.protobuf.json_format import MessageToDict
from internal.proto import user_pb2
from internal.proto import user_pb2_grpc
from services.recommendations import Affinity
from services.queue import Queue


class InteractionsService(user_pb2_grpc.InteractionsServiceServicer):
    def __init__(self, affinity: Affinity, queue: Queue):
        self.affinity = affinity
        self.queue = queue

    def ProcessData(self, request, context):
        try:
            decode_request = MessageToDict(request)
            user_id = decode_request.get('user')
            data = self.affinity.process(decode_request, {})

            processed_data = {
                "user": user_id,
                "movies": data
            }
            result = self.queue.insert_recommendation(user_id, processed_data)
            if result:
                return user_pb2.SuccessResponse(success=True)
            raise
        except Exception as e:
            print(e)
            return user_pb2.SuccessResponse(success=False)
