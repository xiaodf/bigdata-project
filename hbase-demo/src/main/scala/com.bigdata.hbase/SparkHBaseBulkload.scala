package com.sohu.userapp.anticheat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xdf on 2019/1/9.
  */
object SparkHBaseBulkload {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("com").setLevel(Level.ERROR) //设置日志级别

    //日期定义
    val today = args(0)
    val hbaseTabName=args(1)
    val hdfsTmpPath = args(2)

    //创建SparkSession
    val sparkconf = new SparkConf().setAppName("UserMetrics").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkconf)
    val spark = SparkSessionSingleton.getInstance(sc.getConf)

    //配置hbase参数
    val conf = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", "dsrv2.heracles.sohuno.com,drm2.heracles.sohuno.com,dmeta2.heracles.sohuno.com,dmeta1.heracles.sohuno.com,drm1.heracles.sohuno.com")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase-secure")
    conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTabName)

    // 获取用户userid,手机号,数美指标导入hbase
    val userid_phone_shumei_daily_rdd = get_userid_phone_shumei_daily(spark,today)

    //获取用户维度的广告曝光数和点击率导入hbase
    val ad_exposure_click_rdd = get_ad_exposure_click(spark,today)

    //整合用户指标
    val user_rdd = userid_phone_shumei_daily_rdd
      .union(ad_exposure_click_rdd)

    //spark bulkload导用户指标数据入hbase
    spark_bulkload_to_hbase(user_rdd,conf,hdfsTmpPath)

    sc.stop()
  }

  /**
    * 获取每日活跃用户userid,手机号,数美指标
    * 用到表infodata.newsinfo_ds_ods_ds_anticheat_result,infodata.sq_dim_fulluser_daily
    */
  def get_userid_phone_shumei_daily(spark: SparkSession,today: String) :RDD[(ImmutableBytesWritable, Put)] = {
    val sql = "select a.userid,a.phone_number,b.riskScore,b.blackLevel from (select userid,phone_number from infodata.sq_dim_fulluser_daily where dt="+today+" and dt=last_date) a " +
      "left join (select mobile,get_json_object(callresult,'$.riskScore') riskScore,get_json_object(callresult,'$.blackLevel') blackLevel " +
      "from infodata.newsinfo_ds_ods_ds_anticheat_result ) b on a.phone_number=b.mobile"
    val data = spark.sql(sql)

    val userid_phone_shumei_rdd = data.rdd.map(record => {
      val userid = record.getString(0)
      val rowkey = userid+"_"+today
      val put = new Put(Bytes.toBytes(rowkey))

      try{
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("dt"), Bytes.toBytes(today))
        val phone = record.get(1)
        if( phone != None && phone != null ){
          put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("phone"), Bytes.toBytes(phone.toString))
        }
        val risk_score = record.get(2)
        if( risk_score != None && risk_score != null ){
          put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("risk_score"), Bytes.toBytes(risk_score.toString))
        }
        val black_level = record.get(3)
        if( black_level != None && black_level != null ){
          put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("black_level"), Bytes.toBytes(black_level.toString))
        }
      }catch {
        case e: Exception => println(s"================================ ${e.printStackTrace()}")
      }

      (new ImmutableBytesWritable, put)
    })
    userid_phone_shumei_rdd
  }

  /**
    * 初始化的时候跑一次
    * 获取全量历史用户userid,手机号,数美指标
    * 用到表infodata.newsinfo_ds_ods_ds_anticheat_result,infodata.sq_dim_fulluser_daily
    */
  def get_userid_phone_shumei(spark: SparkSession,today: String) :RDD[(ImmutableBytesWritable, Put)] = {
    val sql = "select a.userid,a.phone_number,b.riskScore,b.blackLevel from (select userid,phone_number from infodata.sq_dim_fulluser_daily where dt="+today+") a " +
      "left join (select mobile,get_json_object(callresult,'$.riskScore') riskScore,get_json_object(callresult,'$.blackLevel') blackLevel " +
      "from infodata.newsinfo_ds_ods_ds_anticheat_result ) b on a.phone_number=b.mobile"
    val data = spark.sql(sql)

    val userid_phone_shumei_rdd = data.rdd.map(record => {
      val userid = record.getString(0)
      val rowkey = userid+"_"+today
      val put = new Put(Bytes.toBytes(rowkey))

      try{
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("dt"), Bytes.toBytes(today))
        val phone = record.get(1)
        if( phone != None && phone != null ){
          put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("phone"), Bytes.toBytes(phone.toString))
        }
        val risk_score = record.get(2)
        if( risk_score != None && risk_score != null ){
          put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("risk_score"), Bytes.toBytes(risk_score.toString))
        }
        val black_level = record.get(3)
        if( black_level != None && black_level != null ){
          put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("black_level"), Bytes.toBytes(black_level.toString))
        }
      }catch {
        case e: Exception => println(s"================================ ${e.printStackTrace()}")
      }

      (new ImmutableBytesWritable, put)
    })
    userid_phone_shumei_rdd
  }

  /**
    * 获取当日活跃用户维度的(广告曝光数,广告点击率)
    * 用到表infodata.sq_raw_log
    */
  def get_ad_exposure_click(spark: SparkSession,today: String) : RDD[(ImmutableBytesWritable, Put)] = {
    val sql = "select userid,ad_exposure,if(ad_click_rate is NULL,0,ad_click_rate) ad_click_rate from (select a.uuid userid,ad_exposure,round(ad_click/ad_exposure,3) ad_click_rate from (select uid.uuid uuid,count(*) ad_exposure from infodata.sq_raw_log t " +
      "where dt="+today+" and event='36' group by uid.uuid)a left join(select uid.uuid uuid,count(*) ad_click from infodata.sq_raw_log t where dt="+today+" and event='37'" +
      " group by uid.uuid) b on a.uuid=b.uuid) c "
    val data = spark.sql(sql)

    val ad_exposure_click_rdd = data.rdd.map(record => {
      val userid = record.getString(0)
      val rowkey = userid+"_"+today
      val put = new Put(Bytes.toBytes(rowkey))

      try{
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("dt"), Bytes.toBytes(today))

        val ad_exposure = record.get(1)
        if( ad_exposure != None && ad_exposure != null ) {
          put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("ad_exposure"), Bytes.toBytes(ad_exposure.toString))
        }
        val ad_click_rate = record.get(2)
        if( ad_click_rate != None && ad_click_rate != null ) {
          put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("ad_click_rate"), Bytes.toBytes(ad_click_rate.toString))
        }
      }catch {
        case e: Exception => println(s"================================ ${e.printStackTrace()}")
      }

      (new ImmutableBytesWritable, put)
    })
    ad_exposure_click_rdd
  }
  /**
    * spark bulkload导dataframe数据入hbase
    */
  def spark_bulkload_to_hbase(rdd: RDD[(ImmutableBytesWritable, Put)], conf: Configuration,path: String): Unit ={
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //save to hbase hfile
    job.getConfiguration.set("mapred.output.dir", path)
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
    * 创建单例模式的SparkSession
    */
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
