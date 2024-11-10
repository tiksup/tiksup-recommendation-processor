from redis import StrictRedis
from os import getenv
import json


class RedisConnection:
    def __init__(self) -> None:
        try:
            self.rdb = StrictRedis(
                host=getenv('REDIS_HOST', 'localhost'),
                port=getenv('REDIS_PORT', '6379'),
                db=0
            )
            self.rdb.ping()
            print('Connect to database')
        except Exception as e:
            print(f'Error connecting to Redis: {e}')
            raise 

    def insert_recommendation(self, user_id: str, data: dict):
        try:
            encoded_data = json.dumps(data)
            self.rdb.set(f'user:{user_id}:recommendations', encoded_data)
            print('Insertion success')
        except Exception as e:
            print('error to insert data: {e}')
