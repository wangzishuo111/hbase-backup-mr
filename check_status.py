import os
import sys
import monitor
from redis_pool import REDIS

def get_status(date, table_name, hdfs_path, nas_path):
    project = table_name
    cmd_nas = "du -b %s/%s_%s/*| awk '{print $1}'" %(nas_path, table_name, date)
    print cmd_nas
    ret = os.popen(cmd_nas).read()
    list_ret = ret.splitlines()
    ret_nas = map(int, list_ret)
    ret_nas = sum(ret_nas)

    cmd_hdfs = "/opt/hadoop-3.1.0/bin/hadoop fs -du %s | awk '{print $1}'" %hdfs_path
    print cmd_hdfs
    ret_hdfs = os.popen(cmd_hdfs).read()
    list_ret_hdfs = ret_hdfs.splitlines()
    ret_hdfs = map(int, list_ret_hdfs)
    ret_hdfs = sum(ret_hdfs)
    print 'ready to set redis'
    if ret_nas == ret_hdfs:
        print 'set %s status is 0' %(project)
        redis = REDIS()
        redis.set_one(date, project, '0')
    else:
        redis = REDIS()
        print 'set %s status is 2' %(project)
        redis.set_one(date, project, '2')
        monitor.monitor(project)
    redis_status = redis.get_one(date, project)
    if not redis_status:
        monitor.monitor('redis set error[%s]'%project)
    print ret_nas, ret_hdfs


if __name__ == '__main__':
    date = sys.argv[1]
    table_name = sys.argv[2]
    hdfs_path = sys.argv[3]
    nas_path = sys.argv[4]
    #date = '20190803' 
    #table_name = 'mark_table_prd' 
    #hdfs_path = '/kd-op/wzs/hbase_backup/mark_table/output' 
    #nas_path = '/mnt/Hbase_backup/' 
    get_status(date, table_name, hdfs_path, nas_path)
