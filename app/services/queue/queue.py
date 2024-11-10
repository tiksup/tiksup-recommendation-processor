import json
from redis import StrictRedis


class Queue:
    def __init__(self, rdb: StrictRedis) -> None:
        self.rdb = rdb

    def insert_recommendation(self, user_id: str, data: dict) -> bool:
        try:
            encoded_data = json.dumps(data)
            self.rdb.set(f'user:{user_id}:recommendations', encoded_data)
            print('Insertion success')
            return True
        except Exception as e:
            print(f'error to insert data: {e}')
            return False
