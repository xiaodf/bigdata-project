#!/bin/bash

source ~/.bash_profile

today=$(date "+%Y%m%d")

if [ $# -gt 0 ];then
    today=$1
fi
echo $today

secondday=(`date -d "$today -1 day" +"%Y%m%d"`)
echo $secondday

secondday2=(`date -d "$today -2 day" +"%Y%m%d"`)
echo $secondday2

threedaysBefore=(`date -d "$today -3 day" +"%Y%m%d"`)
echo $threedaysBefore

start_time=$(date -d today +"%Y-%m-%d %H:%M:%S")

########################同步后端cid相关数据#####################
#rm -rf /home/infodata/data/etl/etlshell/push/region_push/cid_device.txt
#mysql -hcomt-tongji.db2.sohuno.com -ucomment_tongji -pegJ49rtuXszp7Bg5 -N -e "select cid,did,os,cid_type from comments.cid_device;" > /home/infodata/data/etl/etlshell/push/region_push/cid_device.txt
#sleep 30s
#hdfs dfs -rm -r -f hdfs://dc5/user/infodata/hive/online/server/sq_server_regionpush_cid_device/cid_device.txt
#hdfs dfs -put /home/infodata/data/etl/etlshell/push/region_push/cid_device.txt  hdfs://dc5/user/infodata/hive/online/server/sq_server_regionpush_cid_device/

sqoop import -D mapred.job.queue.name=datacenter --connect "jdbc:mysql://comt-tongji.db2.sohuno.com:3306/comments" --username comment_tongji --password egJ49rtuXszp7Bg5 --table cid_device  --columns  "cid,did,os,cid_type" --delete-target-dir --beeline "jdbc:hive2://dsrv2.heracles.sohuno.com:10000/infodata;principal=hive/dsrv2.heracles.sohuno.com@HERACLE.SOHUNO.COM;" --hive-import --fields-terminated-by '\t' --hive-database infodata  --hive-table sq_server_regionpush_cid_device  --hive-overwrite --num-mappers 1

impala-shell -i dmeta2.heracles.sohuno.com:25003  -k -d infodata -q " invalidate metadata infodata.sq_server_regionpush_cid_device;" 

########################获取最新task_id############################
task_id=$today"1"

impala-shell -i dmeta2.heracles.sohuno.com:25003  -k -d infodata -q "invalidate metadata infodata.newsinfo_service_finish_check;select task_id from infodata.newsinfo_service_finish_check order by task_id desc limit 1;" -o /home/infodata/data/etl/etlshell/push/region_push/taskid.txt

taskid=$(cat /home/infodata/data/etl/etlshell/push/region_push/taskid.txt | sed -n '5p' | awk '{print $2}')

tmpid=${taskid:0:8}

if [ $today -eq $tmpid ];
then
	#如何当天已有任务执行过，任务id加1
	n1=${taskid:8:9}
        n2=$(($n1+1))
        task_id=$today$n2 
fi
echo $task_id

#######################生成地域码&cid结果数据#####################

#20181205
hivesql="insert overwrite table infodata.newsinfo_push_region_data partition (task_id=$task_id)
select f.cid cid,f.cid_type cid_type,f.os os ,d.city tag from (select if(a.did is null,b.did,a.did) did, if(a.city is null,b.city,a.city) city  from (select c.did did,c.city city from (select uid.did did,substr(geo_info.city,1,4) city ,row_number() over (partition by uid.adid order by log_time.submit asc) rnk from infodata.sq_raw_log where dt=$secondday and geo_info.city !='') c where c.rnk=1) a full join (select distinct(uid.did) did,geo_info.city city from infodata.sq_dim_user_daily where dt=$secondday2 and geo_info.city !='unknown') b on a.did=b.did) d left join (select cid,did,os,cid_type from infodata.sq_server_regionpush_cid_device) f on d.did=f.did where f.did is not null;"

/usr/bin/beeline -u 'jdbc:hive2://dsrv2.heracles.sohuno.com:10000/infodata;principal=hive/dsrv2.heracles.sohuno.com@HERACLE.SOHUNO.COM;' --verbose=true  --hiveconf mapreduce.job.queuename=info -e "$hivesql"
impala-shell -i dmeta2.heracles.sohuno.com:25003  -k -d infodata -q "invalidate metadata infodata.newsinfo_push_region_data"

#######################生成任务完成验证表数据#######################
end_time=$(date -d today +"%Y-%m-%d %H:%M:%S")

#验证下结果表是否有数据
/usr/bin/beeline -u 'jdbc:hive2://dsrv2.heracles.sohuno.com:10000/infodata;principal=hive/dsrv2.heracles.sohuno.com@HERACLE.SOHUNO.COM;' --verbose=true  --hiveconf mapreduce.job.queuename=datacenter -e "select count(*) from infodata.newsinfo_push_region_data where task_id=$task_id;" > /home/infodata/data/etl/etlshell/push/region_push/finish.txt
sleep 3s
count=$(cat /home/infodata/data/etl/etlshell/push/region_push/finish.txt | sed -n '4p' | awk '{print $2}')
echo $count

if [ $count -eq 0 ];
  then
    #结果表为空，发送告警
    curl http://10.16.49.181:8011?msg="大集群-push需求——今日地域push结果表为空"
  else
   #删除3天前历史数据,只保留近三天数据
   hdfs dfs -rm -r -f hdfs://dc5/user/infodata/hive/online/newsinfo_push_region_data/task_id=$threedaysBefore*

   /usr/bin/beeline -u 'jdbc:hive2://dsrv2.heracles.sohuno.com:10000/infodata;principal=hive/dsrv2.heracles.sohuno.com@HERACLE.SOHUNO.COM;' --verbose=true  --hiveconf mapreduce.job.queuename=datacenter -e "insert into table infodata.newsinfo_service_finish_check values (2,'infodata.newsinfo_push_region_data',$task_id,'$start_time','$end_time');"

   impala-shell -i dmeta2.heracles.sohuno.com:25003  -k -d infodata -q "invalidate metadata infodata.newsinfo_push_region_data;invalidate metadata infodata.newsinfo_service_finish_check"

fi

if [ $? -eq 0 ];then
    echo 0
else
    exit 1
fi
