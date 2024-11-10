from redis import StrictRedis
from os import getenv


class RedisConnection:
    def __init__(self) -> None:
        try:
            self.__rdb = StrictRedis(
                host=getenv('REDIS_HOST', 'localhost'),
                port=getenv('REDIS_PORT', '6379'),
                db=0
            )
            if self.__rdb.ping():
                print('Connect to database')
                return
            raise
        except Exception as e:
            print(f'Error connecting to Redis: {e}')
            raise
    
    def connect(self) -> StrictRedis:
        return self.__rdb