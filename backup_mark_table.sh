hdfs_input_file='/kd-op/wzs/hbase_backup/input/mark_table_regions'
hdfs_output_path='/kd-op/wzs/hbase_backup/mark_table/output'
nas_output_path='/mnt/data2/hbase_backup'
table_name='mark_table_prd'
nowdata=`date +%Y%m%d`
basepath=$(cd `dirname $0`; pwd)
cd ${basepath}
mr_start_time=$(date "+%Y.%m.%d-%H:%M")
echo " mr start time is ${mr_start_time}"

/usr/bin/python redis_pool.py ${nowdata} ${table_name} '2'

/opt/hadoop-3.1.2/bin/hadoop fs -rm -r /kd-op/wzs/hbase_backup/mark_table/output
/usr/bin/python mr_backup_table.py -r hadoop \
--hadoop-bin /opt/hadoop-3.1.0/bin/hadoop \
--hadoop-streaming-jar /opt/hadoop-3.1.0/share/hadoop/tools/lib/hadoop-streaming-3.1.0.jar \
--file backup_mark_table.py \
--file mr_backup_table.py \
--file log.py \
--jobconf mapreduce.job.name=backup_${table_name} \
--jobconf table_name=${table_name} \
--jobconf mapred.max.map.failures.percent=30 \
--jobconf mapreduce.input.lineinputformat.linespermap=1 \
--jobconf mapreduce.reduce.maxattempts=1 \
-o hdfs://${hdfs_output_path} \
hdfs://${hdfs_input_file}

mr_stop_time=$(date "+%Y.%m.%d-%H:%M")
echo "mr stop time is ${mr_stop_time}"

mkdir ${nas_output_path}/mark_table_prd_${nowdata}
echo "mkdir ${nas_output_path}/mark_table_prd_${nowdata}"

/opt/hadoop-3.1.2/bin/hadoop fs -get ${hdfs_output_path}/* ${nas_output_path}/mark_table_prd_${nowdata} 

/usr/bin/python check_status.py ${nowdata} ${table_name} ${hdfs_output_path} ${nas_output_path}

copy_stop_time=$(date "+%Y.%m.%d-%H:%M")
echo "copy stop time is ${copy_stop_time}"
echo "--------------------------------- over ------------------------------------"
