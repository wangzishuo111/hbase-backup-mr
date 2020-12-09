#!/usr/bin/env python 
# -*- coding: utf-8 -*-

import base64
import httplib
import json
import traceback
import kdhbase.connection as kdconn 

from log import *
import os
import sys
import re
import requests


__THRIFT_HOST = 'localhost'
__THRIFT_PORT = 6666
__TABLE_NAME = os.getenv('table_name') 

thrift_conn = None

def get_thrift_conn():
    global thrift_conn
    if not thrift_conn:
        thrift_conn = kdconn.Connection(__THRIFT_HOST, __THRIFT_PORT) 
    return thrift_conn

def thrift_reconn():
    global thrift_conn
    if thrift_conn:
        thrift_conn.close()
        thrift_conn = kdconn.Connection(__THRIFT_HOST, __THRIFT_PORT)

def dump_one(row_start, row_stop, last_rowkey, count):
    thrift_reconn()
    conn = get_thrift_conn()
    table = conn.table(__TABLE_NAME)
    try:
        if last_rowkey:
            row_start = last_rowkey
            logger().info('start scan trackid[%s]', last_rowkey)
        logger().info('start scan start_row:%s, stop_row:%s', row_start, row_stop)
        for row_data in table.scan(row_start = row_start, row_stop = row_stop, batch_size = 10, attributes={'username':'kdadmin','password':'kdadmin'}):
            rowkey = row_data[0]
            total_data = row_data[1]
            #mei you base64 jiami 
           # #split_list = re.split('-|_', rowkey)
           # for k, v in total_data.items():
           #     total_data[k] = base64.b64encode(v) 
           # #if len(split_list) != 8 or rowkey.split('_')[3] == "70":
            print '%s\t%s' %(rowkey, total_data)
            if count %1000 ==0 :
                logger().info('image backup success rowkey is %s, finish num is %s',  rowkey, count+1)
            count += 1
            last_rowkey = rowkey

    except Exception, e:
        logger().error(traceback.format_exc())
        return False, last_rowkey, count
    return True, last_rowkey, count

def backup_table(row_start, row_stop):
    retry_time = 20000 
    succ = False
    last_rowkey = None
    count = 0
    for i in range(retry_time):
        ret, last_rowkey, count = dump_one(row_start, row_stop, last_rowkey, count)   
        if ret:
            succ = True
            logger().info('last rowkey[%s] retry %d times', last_rowkey, i)
            break
    if not succ:
        exit(1)
        logger().info('failed %d times', retry_time)

def main():
    rowkey = '8-45814_20180809115423223-45814_20180809115439795566_00_006_webp'
    stop_rowkey = '8-45814_20180809115423223-45814_20180809115439795566_00_006_webp'
    backup_table(rowkey, stop_rowkey)


if __name__ == '__main__':
    main()
