# -*- coding:utf-8 -*

import traceback
import time
import redis
import sys

class REDIS:
    def __init__(self):
        redis_host = '10.11.5.137'
        port = 6379
        db = 1
        self.client = redis.Redis(host=redis_host, port=port,db=db)

    def get_all(self, date):
        ret = self.client.hgetall(date) 
        return ret
    
    def get_one(self, date, project):
        ret = self.client.hget(date, project)
        return ret
    
    def exist(self, date, project):
        ret = self.client.hexists(date, project)
        return ret
    
    def set_one(self, date, project, status):
        ret = self.client.hset(date, project, status)
        return ret

if __name__ == '__main__':
    redis = REDIS()
    today = sys.argv[1]
    project = sys.argv[2]
    status = sys.argv[3]
    print redis.set_one(today, project, status)

