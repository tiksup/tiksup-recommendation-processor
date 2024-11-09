import grpc
from internal.proto import data_pb2
from internal.proto import data_pb2_grpc


class InteractionsService(data_pb2_grpc.InteractionsServiceServicer):
    def ProcessData(self, request, context):
        print(request.data)
        return data_pb2.SuccessResponse(success=True)
