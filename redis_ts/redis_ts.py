#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 00:25:23 2021

@author: onebula
"""

import time
import json
import redis

from ciso8601 import parse_datetime


class RedisTS(object):
    def __init__(self, ip, port, pwd, 
                 ts_ex=3600,
                 trim_num=None,
                 trim_time_window=None,
                ):
        self.ip, self.port, self.pwd = ip, port, pwd
        self.ts_ex = ts_ex
        self.trim_num = trim_num
        self.trim_time_window = trim_time_window
        self.connect(self.ip, self.port, self.pwd)
    
    def connect(self, ip, port, pwd):
        self.pool = redis.ConnectionPool(host=ip, port=port, password=pwd, decode_responses=True)
        self.redis = redis.Redis(connection_pool=self.pool)
    
    def datestr2timestamp(self, datestr):
        timestamp = parse_datetime(datestr).timestamp() if isinstance(datestr, str) else datestr
        return timestamp
    
    def data2kv(self, ts_data):
        ts_kv = {json.dumps([t, data], separators=(',', ':')): self.datestr2timestamp(t)
                 for t, data in sorted(ts_data, key=lambda item: item[0])
                }
        return ts_kv
    
    def res2data(self, res):
        ts_data = [json.loads(item) for item in res]
        return ts_data
    
    def record(self, ts_name, data, t=None, ex=None):
        t = int(time.time()) if t is None else t
        ts_data = [(t, data)]
        self.redis.zadd(ts_name, ts_data)
        self.redis.expire(ts_name, self.ts_ex if ex is None else ex)
    
    def update_ts(self, ts_name, ts_data, ex=None):
        # 更新时间序列，ts_data不要求按时间排序
        ts_kv = self.data2kv(ts_data)
        self.redis.zadd(ts_name, ts_kv)
        self.redis.expire(ts_name, self.ts_ex if ex is None else ex)
        
    def get_len(self, ts_name):
        # 获取时间序列长度
        return self.redis.zcard(ts_name)
    
    def get_last(self, ts_name, with_timestamp=False):
        # 获取最新的数据
        last = self.redis.zrange(ts_name, start=-1, end=-1, withscores=with_timestamp)
        return last
    
    def slicing(self, ts_name, start=0, end=-1):
        # 根据编号筛选数据
        res = self.redis.zrange(ts_name, start, end)
        ts_data = self.res2data(res)
        return ts_data
    
    def ranging(self, ts_name, start_time, end_time, every=None):
        # 根据时间范围筛选数据
        start_timestamp = self.datestr2timestamp(start_time)
        end_timestamp = self.datestr2timestamp(end_time)
        res = self.redis.zrangebyscore(ts_name, end_timestamp, start_timestamp)
        ts_data = self.res2data(res)
        return ts_data
    
    def sliding_window(self, ts_name, time_window, start_time=None, end_time=None):
        # 获取滑动窗内的数据
        if start_time is not None:
            start_timestamp = self.datestr2timestamp(start_time)
            end_timestamp = start_timestamp + time_window
            return self.ranging(ts_name, start_timestamp, end_timestamp)
        if end_time is not None:
            end_timestamp = self.datestr2timestamp(end_time)
            start_timestamp = end_timestamp - time_window
            return self.ranging(ts_name, start_timestamp, end_timestamp)
        last = self.get_last(ts_name, with_timestamp=True)
        if last:
            end_timestamp, _ = last
            start_timestamp = end_timestamp - time_window
            return self.ranging(ts_name, start_timestamp, end_timestamp)
        else:
            return []
        
    def slicing_trim(self, ts_name, num):
        # 裁剪时间序列，仅保留最近的num个数据
        size = self.get_len(ts_name)
        self.redis.zremrangebyrank(ts_name, 0, max(0, size - num - 1))
        
    def ranging_trim(self, ts_name, time_window):
        # 裁剪时间序列，仅保留最近的time_window范围内的数据
        last = self.get_last(ts_name, with_timestamp=True)
        if last:
            end_timestamp, _ = last
            self.redis.zremrangebyscore(ts_name, 0, end_timestamp - time_window)
    
    def trim(self, ts_name, num=None, time_window=None):
        trim_num = num if num is not None else self.trim_num
        if trim_num is not None:
            self.slicing_trim(ts_name, trim_num)
        trim_time_window = time_window if time_window is not None else self.trim_time_window
        if trim_time_window is not None:
            self.ranging_trim(ts_name, trim_time_window)
        
        
    
'''Data structure
ts_data: [(t1, data1), (t2, data2), ...]
ts_kv: {(t1, data1): timestamp1,
        (t2, data2): timestamp2,
        ...
       }

ts_name: {timestamp1: (t1, data1),
          timestamp2: (t2, data2),
          ...
         }
'''
    
if __name__ == '__main__':
    pass
