#!/usr/bin/python
# -*- coding: utf-8 -*-

from kdbase.log import logger
#from kdbase import hbase_util_thrift
import kdhbase.connection as kdconn
import sys
import os
import time
import requests

file_path = "/data/lidar_point_cloud_index/"
__THRIFT_HOST = '10.11.5.3'
__THRIFT_PORT = 6666
thrift_conn = None

def sync_lidar_point_cloud_index(row_key, content):
    url = "http://192.168.8.16:9527/prd/kv/set/gzip"
    playload = {"namespace": 'lidar_point_cloud_index', "key": new_key}
    ret = requests.post(url, params=playload, data=content)

def judge_rowkey(row_key):
    pro_key = "_".join(row_key.split('_')[4:])
    after_key = '/'.join(pro_key.split('/')[1:])
    split_key = pro_key.split('/')[0].split('-')
    before_key = split_key[-1]+'-'+split_key[0]
    logger().info('before_key:[%s]' % (before_key))
    id_1 = before_key.split('-')[0]
    id_2 = before_key.split('-')[1]
    logger().info('id_1:[%s] -- id_2:[%s]' % (id_1, id_2))
    if len(id_1) > len(id_2):
        logger().info('id_1:[%s] > id_2:[%s] this is right' % (id_1, id_2))
        return False
    new_key = before_key + '/' + after_key
    return new_key

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

def get_lidar_point(trackid, namespace, hbase_table):
    starttime = time.time()
    for i in range(0, 100):
       start_key = '%02d-%s_%s' % (i, namespace, trackid)
       for i in range(3):
            thrift_reconn()
            conn = get_thrift_conn()
            #hbase_util_thrift.thrift_reconn()
            #conn = hbase_util_thrift.get_thrift_conn()
            table = conn.table(hbase_table)
            try:
                for row_data in table.scan(row_start=start_key, row_stop=start_key + '~', batch_size=30, attributes={'username':'kdadmin','password':'kdadmin'}):
                    row_key = row_data[0]
                    data = row_data[1]
                    logger().info("row_key:[%s]" % (row_key))
                break
            except Exception, e:
                logger().info('exception:%s, retry ', str(e))
    endtime = time.time()
    time_consume = endtime - starttime
    logger().info('scaned time for track_id: [%s] time: [%s]', trackid, time_consume)

def main():
    pass

if __name__ == '__main__':
    namespace = sys.argv[1]
    hbase_table = sys.argv[2]
    get_lidar_point('', namespace, hbase_table)
