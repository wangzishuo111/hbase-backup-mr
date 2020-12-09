#!/usr/bin/env python
#-*- coding: utf-8 -*-

import httplib
from mrjob.job import MRJob
from log import logger
import sys
import os

os.system('/opt/hadoop-3.1.2/bin/kdhadoop fs -get /kd-op/wzs/hbase_backup/kdhbase .')
assert os.path.exists('./kdhbase'), 'download kdhbase error!!!'
#from convert import scan_region
sys.path.append('.')
import backup_mark_table 

class MRWordCounter(MRJob):

    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'

    def mapper(self, key, line):
        region_msg = line.strip().split('\t')[1]
        region_msg = eval(region_msg)
        row_start = region_msg['start_key']
        row_stop = region_msg['end_key']
        row_keys = backup_mark_table.backup_table(row_start, row_stop)

if __name__ == '__main__':
    MRWordCounter.run()

