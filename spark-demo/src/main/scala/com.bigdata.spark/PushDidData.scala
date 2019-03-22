package com.sohu.userapp.spark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.bigdata.spark.SSHCommandExecutor
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.Searching._

/**
  * Created by xdf on 2018/8/6.
  * 大集群跑的程序
  */

object PushDidData {

  case class Data(did: String,item_id: Int)

  def main(args: Array[String]): Unit = {
    @volatile var broadcastRegisterUserDid: Broadcast[Array[String]] = null
    @volatile var broadcastSevenNewUserDid: Broadcast[Array[String]] = null
    @volatile var broadcastToadyStartUserDid: Broadcast[Array[String]] = null
    @volatile var broadcastToadySignUserDid: Broadcast[Array[String]] = null
    @volatile var broadcastSignUserDid1: Broadcast[Array[String]] = null
    @volatile var broadcastSignUserDid2: Broadcast[Array[String]] = null
    @volatile var broadcastLastDateUserDid1: Broadcast[Array[String]] = null
    @volatile var broadcastLastDateUserDid2: Broadcast[Array[String]] = null


    //日期定义
    val now: Date = new Date()
    val today = getDaysBefore(now,0)
    val yesterday = getDaysBefore(now,1)
    val sixDaysBefore = getDaysBefore(now,6)

    val sparkconf = new SparkConf().setAppName("PushDidData").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //val sc = new SparkContext(sparkconf)
    val spark = SparkSessionSingleton.getInstance(sparkconf)

    println("---------提前将符合各规则的用户的did生成广播变量----------")
    //生成7日内注册用户的did 的广播变量
    val currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(now)
    val sixdaysBeforeTime = getDaysBefore2(now,6)+" 00:00:00"
    broadcastRegisterUserDid = broadcast_user_register(spark,currentTime,sixdaysBeforeTime)

    //生成7日内新增用户的did 的广播变量
    broadcastSevenNewUserDid = broadcast_user_new(spark,today,yesterday,sixDaysBefore)

    //生成当日启动过用户的did 的广播变量
    broadcastToadyStartUserDid = broadcast_user_today_start(spark,today)

    //生成今日签到用户的did 的广播变量
    val today_begintime = new SimpleDateFormat("yyyy-MM-dd").format(now)+" 00:00:00"
    broadcastToadySignUserDid = broadcast_user_today_sign(spark,today_begintime)

    //到昨天为止连续签到天数大于0 的用户did 的广播变量
    val yesterdayStart = getDaysBefore2(now,1)+" 00:00:00"
    val yesterdayEnd = getDaysBefore2(now,1)+" 23:59:59"
    broadcastSignUserDid1 = broadcast_user_continue_sign(spark,yesterdayStart,yesterdayEnd)

    //到昨天为止连续签到天数等于7n-1的用户did
    broadcastSignUserDid2 = broadcast_user_continue_sign2(spark,yesterdayStart,yesterdayEnd)

    //距离上次启动间隔不大于3天的用户did
    val threeDaysBefore = getDaysBefore(now,3)
    broadcastLastDateUserDid1 = broadcast_user_last_login(spark,today,yesterday,threeDaysBefore)

    //距离上次启动间隔大于3天小于7天的用户did
    broadcastLastDateUserDid2 = broadcast_user_last_login2(spark,yesterday,threeDaysBefore,sixDaysBefore)

    println("---------输入7日活跃用户did与各规则的did进行匹配,打上push标签----------")

    //输入数据-7日内活跃用户did=前6日内活跃用户+今日当前活跃用户
    val input_data =  spark.sql("select distinct(did)  from (select distinct(uid.did) did from infodata.sq_dim_user_daily where dt="+yesterday+" and last_date>="+sixDaysBefore+"  union all select distinct(uid.did) did from infodata.sq_raw_log where dt="+today+") a")

    val result = input_data.rdd.map{ record =>
      val did = record(0).toString
      var push_id = 0
      //broadcastSevenNewUserDid.value.search(did).isInstanceOf[InsertionPoint]调用scala 二分查找函数,注意,找到返回false
      if(!broadcastSevenNewUserDid.value.search(did).isInstanceOf[InsertionPoint]){
        //是7日新增
        if(!broadcastRegisterUserDid.value.search(did).isInstanceOf[InsertionPoint]){
          //是注册用户
          if(!broadcastToadyStartUserDid.value.search(did).isInstanceOf[InsertionPoint]){
            //今日有启动
            if(broadcastToadySignUserDid.value.search(did).isInstanceOf[InsertionPoint]){
              //今日没有签到
              if(!broadcastSignUserDid1.value.search(did).isInstanceOf[InsertionPoint]){
                //到昨天为止连续签到天数大于0
                if(!broadcastSignUserDid2.value.search(did).isInstanceOf[InsertionPoint]){
                  //到昨天为止连续签到天数不等于7n-1
                  push_id = 3
                }else{
                  //到昨天为止连续签到天数等于7n-1
                  push_id = 4
                }
              }else{
                //到昨天为止连续签到天数等于0
                if(!broadcastLastDateUserDid1.value.search(did).isInstanceOf[InsertionPoint]){
                  //距离上次启动间隔不大于3天
                  push_id = 5
                }else if(!broadcastLastDateUserDid2.value.search(did).isInstanceOf[InsertionPoint]){
                  //距离上次启动间隔大于3天小于7天
                  push_id = 6
                }
              }
            }
          } else {
            //今日没有启动
            push_id = 2
          }
        }else{
          //不是注册用户
          push_id = 1
        }
      }else{
        //不是七日新增
        if(!broadcastToadySignUserDid.value.search(did).isInstanceOf[InsertionPoint]){
          //今日没有签到
          if(broadcastSignUserDid1.value.search(did).isInstanceOf[InsertionPoint]){
            //到昨天为止连续签到天数大于0
            if(!broadcastSignUserDid2.value.search(did).isInstanceOf[InsertionPoint]){
              //到昨天为止连续签到天数不等于7n-1
              push_id = 3
            }else{
              //到昨天为止连续签到天数等于7n-1
              push_id = 4
            }
          }else{
            //到昨天为止连续签到天数等于0
            if(broadcastLastDateUserDid1.value.search(did).isInstanceOf[InsertionPoint]){
              //距离上次启动间隔不大于3天
              push_id = 5
            }else if(broadcastLastDateUserDid2.value.search(did).isInstanceOf[InsertionPoint]){
              //距离上次启动间隔大于3天小于7天
              push_id = 6
            }
          }
        }
      }
      (did,push_id)
    }

    println("---------保存结果到hdfs目录----------")
    //获取最近一次成功的任务id
    val tasks =  spark.sql("select task_id from infodata.push_batch_flag order by task_id desc limit 1").rdd.map{ record => record(0)}.collect()
    var task_id = "20180808001"   //初始化
    if(!tasks.isEmpty){
      task_id = tasks.apply(0).toString
    }
    val new_task_id = getNewTaskID(task_id,today.toString)
    val hdfsOutputPath = args(0) +"/task_id="+new_task_id
    println("----------hdfsOutputPath: "+ hdfsOutputPath)

    //保存结果到hive表对应的hdfs目录
    import spark.implicits._

    val ResultDF = result.map{ record =>
      val did = record._1
      val item_id = record._2
      Data(record._1,record._2)
    }.toDF()

     ResultDF.coalesce(1).write.mode(SaveMode.Overwrite).parquet(hdfsOutputPath)

    println("---------impala添加一个新分区----------")
    //调用impala-shell命令添加新分区
    val sshExecutor: SSHCommandExecutor = new SSHCommandExecutor("10.31.103.113", "infodata", "infodata")
    val cmd: String = "/usr/bin/beeline -u 'jdbc:hive2://dsrv2.heracles.sohuno.com:10000/infodata;principal=hive/dsrv2.heracles.sohuno.com@HERACLE.SOHUNO.COM;' --hiveconf mapreduce.job.queuename=datacenter -e \"alter table infodata.push_batch_data add if not exists partition(task_id='" + new_task_id + "');\""
    sshExecutor.execute(cmd)

    println("---------最后保存一条本任务完成记录到验证表----------")
    //最后保存一条本任务完成记录到验证表
    val task_end_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    spark.sql("insert into table infodata.push_batch_flag values('"+new_task_id+"','"+currentTime+"','"+task_end_time+"') ")

    println("---------刷新impala元数据----------")
    val cmd2: String = "impala-shell -i dmeta2.heracles.sohuno.com:25003 -d infodata -q \"invalidate metadata infodata.push_batch_flag;\""
    sshExecutor.execute(cmd2)
    println("done")

    spark.stop()
  }

  /**
    * 生成每次任务的id,当天已执行过的话id + 1 ,格式为20180808003
    * @param task_id
    * @param today
    * @return
    */
  def getNewTaskID(task_id: String,today : String):String = {
    var new_task_id = today+"001"
    if(today.equalsIgnoreCase(task_id.substring(0,8))){
      //从数据库查的任务id 不为空,且已有当天id, id 递增
      val str2 = task_id.substring(8,11).toInt+1
      import java.text.DecimalFormat
      val f1 = new DecimalFormat("000")
      new_task_id = today+f1.format(str2)
    }
    new_task_id
  }

  /**
    * 指定日期和间隔天数，返回指定日期前N天的日期 date - N days
    * @param dt
    * @param interval
    * @return
    */
  def getDaysBefore(dt: Date, interval: Int):Int = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt)

    cal.add(Calendar.DATE, - interval)
    val day = dateFormat.format(cal.getTime())
    val resInt = Integer.parseInt(day)
    resInt
  }

  def getDaysBefore2(dt: Date, interval: Int):String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt)
    cal.add(Calendar.DATE, - interval)
    val resInt2 = dateFormat.format(cal.getTime())
    resInt2
  }

  /**
    * 生成7日内注册用户did广播变量
    * @return
    */
  def broadcast_user_register(spark: SparkSession,currentTime :String,sixdaysBeforeTime :String) :Broadcast[Array[String]] = {
    @volatile var broadcastRegisterUserDid: Broadcast[Array[String]] = null
    //val spark = SparkSessionSingleton.getInstance(sc.getConf)
    val register_user_did = spark.sql("select b.did from (select id from infodata.tmp_user_zhuce where create_time >='"+sixdaysBeforeTime+"' and create_time<='"+currentTime+"') a left join (select userid,did from infodata.tmp_user_device) b on a.id=b.userid where b.did !='NULL'")

    val user_register = register_user_did.rdd.map { row => {row.getString(0)}}

    if(broadcastRegisterUserDid !=null){
      broadcastRegisterUserDid.unpersist()
    }
    broadcastRegisterUserDid = spark.sparkContext.broadcast(user_register.collect())
    println(s"broadcastRegisterUserDid.size= ${broadcastRegisterUserDid.value.size}")
    broadcastRegisterUserDid
  }

  /**
    * 生成7日内新增用户did广播变量
    * @return
    */
  def broadcast_user_new(spark: SparkSession,today: Int,yesterday: Int,sixDaysBefore: Int) :Broadcast[Array[String]] = {
    @volatile var broadcastSevenNewUserDid: Broadcast[Array[String]] = null

    val new_user_did = spark.sql("select * from (select distinct(a.did) did from  (select distinct(uid.did) did from infodata.sq_raw_log where dt="+today+")a left join (select distinct(uid.did) did from infodata.sq_dim_user_daily where dt="+yesterday+") b on a.did=b.did where a.did is not null and b.did is null union all select distinct(uid.did) did from infodata.sq_dim_user_daily where dt>="+sixDaysBefore+" and dt<="+yesterday+" and is_new=true) e")

    val new_user_did_value = new_user_did.rdd.map { row => {row.getString(0)}}

    if(broadcastSevenNewUserDid !=null){
      broadcastSevenNewUserDid.unpersist()
    }
    broadcastSevenNewUserDid = spark.sparkContext.broadcast(new_user_did_value.collect())
    println(s"broadcastSevenNewUserDid.size= ${broadcastSevenNewUserDid.value.size}")
    broadcastSevenNewUserDid
  }

  /**
    * 生成当日启动用户did广播变量
    * @return
    */
  def broadcast_user_today_start(spark: SparkSession,today: Int) :Broadcast[Array[String]] = {
    @volatile var broadcastToadyStartUserDid: Broadcast[Array[String]] = null
    //val spark = SparkSessionSingleton.getInstance(sc.getConf)
    val today_start_user_did = spark.sql("select distinct(uid.did) did from infodata.sq_raw_log where dt="+today+"")

    val today_start_user_did_value = today_start_user_did.rdd.map { row => {row.getString(0)}}

    if(broadcastToadyStartUserDid !=null){
      broadcastToadyStartUserDid.unpersist()
    }
    broadcastToadyStartUserDid = spark.sparkContext.broadcast(today_start_user_did_value.collect())
    println(s"broadcastToadyStartUserDid.size= ${broadcastToadyStartUserDid.value.size}")
    broadcastToadyStartUserDid
  }

  /**
    * 生成当日签到用户的did 的广播变量
    * @return
    */
  def broadcast_user_today_sign(spark: SparkSession,today_begintime: String) :Broadcast[Array[String]] = {
    @volatile var broadcastToadySignUserDid: Broadcast[Array[String]] = null
    //val spark = SparkSessionSingleton.getInstance(sc.getConf)
    val today_sign_user_did = spark.sql("select distinct(b.did) from (select userid from infodata.tmp_user_sign where last_modify_time>='"+today_begintime+"') a left join (select userid,did from infodata.tmp_user_device) b on a.userid=b.userid where b.did !='NULL'")

    val today_sign_user_did_value = today_sign_user_did.rdd.map { row => {row.getString(0)}}

    if(broadcastToadySignUserDid !=null){
      broadcastToadySignUserDid.unpersist()
    }
    broadcastToadySignUserDid = spark.sparkContext.broadcast(today_sign_user_did_value.collect())
    println(s"broadcastToadySignUserDid.size= ${broadcastToadySignUserDid.value.size}")
    broadcastToadySignUserDid
  }

  /**
    * 生成到昨天为止连续签到天数大于0 的用户did 的广播变量
    * @return
    */
  def broadcast_user_continue_sign(spark: SparkSession,yesterdayStart: String,yesterdayEnd: String ) :Broadcast[Array[String]] = {
    @volatile var broadcastSignUserDid1: Broadcast[Array[String]] = null
    //val spark = SparkSessionSingleton.getInstance(sc.getConf)
    val sign_user_did = spark.sql("select distinct(b.did) from (select userid from infodata.tmp_user_sign where last_modify_time >= '"+yesterdayStart+"' and last_modify_time <='"+yesterdayEnd+"' and sign_count>'0') a left join (select userid,did from infodata.tmp_user_device) b on a.userid=b.userid where b.did !='NULL'")

    val sign_user_did_value = sign_user_did.rdd.map { row => {row.getString(0)}}

    if(broadcastSignUserDid1 !=null){
      broadcastSignUserDid1.unpersist()
    }
    broadcastSignUserDid1 = spark.sparkContext.broadcast(sign_user_did_value.collect())
    println(s"broadcastSignUserDid1.size= ${broadcastSignUserDid1.value.size}")
    broadcastSignUserDid1
  }

  /**
    * 生成到昨天为止连续签到天数等于7n-1的用户did的广播变量
    * @return
    */
  def broadcast_user_continue_sign2(spark: SparkSession,yesterdayStart: String,yesterdayEnd: String) :Broadcast[Array[String]] = {
    @volatile var broadcastSignUserDid2: Broadcast[Array[String]] = null
    //val spark = SparkSessionSingleton.getInstance(sc.getConf)
    val sign_user_did2 = spark.sql("select distinct(b.did) from (select userid from infodata.tmp_user_sign where last_modify_time >= '"+yesterdayStart+"' and last_modify_time <='"+yesterdayEnd+"'  and (cast(sign_count as int)+1) % 7=0) a left join (select userid,did from infodata.tmp_user_device) b on a.userid=b.userid where b.did !='NULL'")

    val sign_user_did2_value = sign_user_did2.rdd.map { row => {row.getString(0)}}

    if(broadcastSignUserDid2 !=null){
      broadcastSignUserDid2.unpersist()
    }
    broadcastSignUserDid2 = spark.sparkContext.broadcast(sign_user_did2_value.collect())
    println(s"broadcastSignUserDid2.size= ${broadcastSignUserDid2.value.size}")
    broadcastSignUserDid2
  }

  /**
    * 生成距离上次启动间隔不大于3天的用户did 的广播变量
    * @return
    */
  def broadcast_user_last_login(spark: SparkSession,today: Int,yesterday:Int,threeDaysBefore:Int) :Broadcast[Array[String]] = {
    @volatile var broadcastLastDateUserDid1: Broadcast[Array[String]] = null
    //val spark = SparkSessionSingleton.getInstance(sc.getConf)
    val lastdate_user_did1 = spark.sql("select distinct(did)  from (select distinct(uid.did) did from infodata.sq_dim_user_daily where dt="+yesterday+" and last_date>="+threeDaysBefore+"  union all select distinct(uid.did) did from infodata.sq_raw_log where dt="+today+") a ")

    val lastdate_user_did1_value = lastdate_user_did1.rdd.map { row => {row.getString(0)}}

    if(broadcastLastDateUserDid1 !=null){
      broadcastLastDateUserDid1.unpersist()
    }
    broadcastLastDateUserDid1 = spark.sparkContext.broadcast(lastdate_user_did1_value.collect())
    println(s"broadcastLastDateUserDid1.size= ${broadcastLastDateUserDid1.value.size}")
    broadcastLastDateUserDid1
  }


  /**
    * 生成距离上次启动间隔大于3天小于7天的用户did 的广播变量
    * @return
    */
  def broadcast_user_last_login2(spark: SparkSession,yesterday:Int,threeDaysBefore:Int,sixDaysBefore: Int) :Broadcast[Array[String]] = {
    @volatile var broadcastLastDateUserDid2: Broadcast[Array[String]] = null
    //val spark = SparkSessionSingleton.getInstance(sc.getConf)
    val lastdate_user_did2 = spark.sql("select distinct(uid.did) did from infodata.sq_dim_user_daily where dt="+yesterday+" and last_date<"+threeDaysBefore+" and last_date>="+sixDaysBefore+"")

    val lastdate_user_did2_value = lastdate_user_did2.rdd.map { row => {row.getString(0)}}

    if(broadcastLastDateUserDid2 !=null){
      broadcastLastDateUserDid2.unpersist()
    }
    broadcastLastDateUserDid2 = spark.sparkContext.broadcast(lastdate_user_did2_value.collect())
    println(s"broadcastLastDateUserDid2.size= ${broadcastLastDateUserDid2.value.size}")
    broadcastLastDateUserDid2
  }

  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
          .enableHiveSupport()
          .getOrCreate()
      }
      instance
    }
  }

}
