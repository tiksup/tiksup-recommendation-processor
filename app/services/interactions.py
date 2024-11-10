from internal.proto import user_interacctions_pb2 as user_pb2
from internal.proto import user_interacctions_pb2_grpc as user_pb2_grpc
from google.protobuf.json_format import MessageToDict
import json


class InteractionsService(user_pb2_grpc.InteractionsServiceServicer):
    def ProcessData(self, request, context):
        try:
            decode_request = MessageToDict(request)
            print(decode_request)
        except Exception as e:
            print(e)

        return user_pb2.SuccessResponse(success=True)
