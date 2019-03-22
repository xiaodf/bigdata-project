package com.bigdata.sparkstreaming

import java.io.ByteArrayInputStream
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.sohu.proto.rawlog.Log
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

import scala.collection.Searching._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xdf on 2018/11/9.
  * 功能：
  * SparkStreaming实时程序将当日新增用户信息存入kudu表
  * 利用redis进行用户去重判断
  */
object UserAppChannelAddCorrectRedis {

  val SOHU_UID_HDFS_PATH = "hdfs://dc5/user/infodata/hive/online/user_model/"
  var groupId = "spark_kafka_streaming_user_app_channeladd_correct_201801126"
  var kuduMaster = "dmeta1.heracles.sohuno.com:7051"
  var kuduTable = "newsinfo_kudu_fact_channel_correct"
  val redisKey = "newsinfo:channeldids"


  def main(args: Array[String]) {
    var maxRate = "3000"
    var duration = 60
    var partitionNum = 100
    var topics = "zhiqu"

    if (args.length < 4) {
      println("usage:topic,groupid,kuduMaster,countkuduTable,maxRate,duration,partitionNum")
      System.exit(-1)
    }
    topics = args(0)
    groupId = args(1)
    kuduMaster = args(2)
    kuduTable = args(3)

    if (args.length >= 5)
      maxRate = args(4)
    if (args.length >= 6)
      duration = args(5).toInt
    if (args.length >= 7)
      partitionNum = args(6).toInt

    val ssc = dealWithKafkaManager(topics, duration, maxRate, partitionNum)

    ssc.start()
    ssc.awaitTermination()
  }

  def dealWithKafkaManager(topics: String, duration: Long, maxRate: String, partitionNum: Int): StreamingContext = {
    val sc = new SparkConf().setAppName("UserAppChannelAdd-" + topics).
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.set("spark.streaming.kafka.maxRatePerPartition", maxRate)
    sc.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(sc, Seconds(duration))
    val topicsSet = topics.split(",").toSet
    val kafkaParam = KafkaConfig.createKafkaConfigParam(groupId)
    val kc = new KafkaManager(kafkaParam)

    val lines = kc.createDirectStream[String, Array[Byte], kafka.serializer.StringDecoder, kafka.serializer.DefaultDecoder](ssc, topicsSet)

    lines.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {

        val logs = rdd.repartition(partitionNum).flatMap { l =>
          val jedis = RedisClient.pool.getResource
          val data = ArrayBuffer[String]()
          val value = l._2

          val input = new ByteArrayInputStream(value)
          while (input.available() > 0) {
            try {
              val device = Log.parseDelimitedFrom(input)
              val eventNumber = device.getEvent.getNumber
              if (judgeEvent(eventNumber)) {
                val userDid = device.getUid.getDid //用户
                val create_time = if (device.getLogTime.getSubmit == 0) formatTime(System.currentTimeMillis()) else formatTime(device.getLogTime.getSubmit * 1000l)
                val os = device.getPhoneMeta.getOs.getNumber.toString
                val channel = if (device.getAppMeta.getChannel == "") "-1" else device.getAppMeta.getChannel
                val app_name = device.getAppMeta.getAppName.getNumber.toString
                /*                if (!jedis.sismember("userdid", userDid) && create_time.substring(0, 8) == formatTime(System.currentTimeMillis()).substring(0, 8)) {
                                  //new did add into redis set
                                  jedis.sadd("userdid", userDid)
                                  val line = s"$create_time,$os,$channel,$userDid,$app_name"
                                  data += line
                                }*/
                if (create_time.substring(0, 8) == formatTime(System.currentTimeMillis()).substring(0, 8)) {
                  val value = jedis.hget(redisKey, userDid)
                  if (value == null || ((value == "" || value == "-1") && channel != "-1")) {
                    jedis.hset(redisKey, userDid, channel)
                    val line = s"$create_time,$os,$channel,$userDid,$app_name"
                    data += line
                  }
                }
              }
            } catch {
              case e: Exception => println(s"====================================================== ${e.printStackTrace()}")
            }
          }
          input.close()
          jedis.close()
          data
        }.map { l =>
          val arr = org.apache.commons.lang.StringUtils.splitByWholeSeparatorPreserveAllTokens(l, ",") //l.split(",")
          UserAppLog(arr(0), arr(1), arr(2), arr(3), arr(4))
        }.persist(StorageLevel.MEMORY_AND_DISK)

        val lineSize = logs.collect().length
        println("new dids num " + lineSize)

        if (!logs.partitions.isEmpty) {
          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

          val logsDF = logs.toDF()
          logsDF.repartition(100).createOrReplaceTempView("user_app_logs")

          val allKpiSql = "select create_time,reflect(\"java.util.UUID\", \"randomUUID\") uuid,os,channel,userdid,app_name " +
            "from user_app_logs where userdid is not null group by create_time,os,channel,userdid,app_name"
          val allKpiDF = spark.sql(allKpiSql)

          val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)
          kuduContext.insertIgnoreRows(allKpiDF, kuduTable)

        }
        logs.unpersist()
        kc.updateZKOffsets(rdd)
      }
    }
    ssc
  }

  case class UserAppLog(create_time: String, os: String, channel: String, userdid: String, app_name: String)

  def judgeEvent(event: Int): Boolean = {
    val eventsList = List(4, 5, 6, 7, 8, 9, 11, 15, 16, 17, 18, 19, 21, 22, 25, 26, 27, 29, 30, 32, 35, 36, 37, 38)
    val ifCount = eventsList.search(event).isInstanceOf[Found]
    ifCount
  }

  def formatTime(seconds: Long): String = {
    org.apache.commons.lang3.time.DateFormatUtils.format(new Date(seconds), "yyyyMMddHHmm")
  }

  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

}
