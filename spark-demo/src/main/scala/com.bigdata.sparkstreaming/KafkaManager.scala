package com.bigdata.sparkstreaming

import java.text.SimpleDateFormat
import java.util.Date

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}

import scala.reflect.ClassTag

class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {
  private val kc = new KafkaCluster(kafkaParams)


  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag](ssc: StreamingContext, topics: Set[String]): InputDStream[(K, V)] = {
    val groupId = kafkaParams("group.id")
    //在读取数据前，根据实际情况更新ZK上offsets
    setOrUpdateOffsets(topics, groupId)

    //从zk上读取message
    val messages = {
      val partationsE = kc.getPartitions(topics)
      if (partationsE.isLeft) throw new SparkException("get kafka partation failed ")
      val partations = partationsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partations)
      if (consumerOffsetsE.isLeft) throw new SparkException("get kafka consumer offsets failed ")
      val consumerOffsets = consumerOffsetsE.right.get
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, consumerOffsets, (mm: MessageAndMetadata[K, V]) => (mm.key, mm.message))
    }
    messages
  }

  private def setOrUpdateOffsets(topic: Set[String], groupId: String): Unit = {
    val partitionsE = kc.getPartitions(topic)
    if (partitionsE.isLeft) throw new SparkException("get kafka partition failed")

    val partitions = partitionsE.right.get
    val consumerOffsectE = kc.getConsumerOffsets(groupId, partitions)
    val hasConsumed = if (consumerOffsectE.isLeft) false else true

    val reset = kafkaParams("auto.offset.reset")

    if (hasConsumed) {
      //已消费过
      //获取zk上已经消费的Offset
      val consumerOffsets = consumerOffsectE.right.get

      val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
      val lastestLeaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get

      println(s"kafka smallest offset is $earliestLeaderOffsets , consumer offset is $consumerOffsets")
      var offsets: Map[TopicAndPartition, Long] = Map()

      consumerOffsets.foreach {
        case (tp, offset) =>
          val earliestLeaderOffsets2 = earliestLeaderOffsets(tp).offset
          val lastestLeaderOffsets2 = lastestLeaderOffsets(tp).offset

          if (offset < earliestLeaderOffsets2 || offset > lastestLeaderOffsets2) {
            //如果已经消费的offset比当前kafka最早的还小或大于当前最大offset
            if (reset == kafka.api.OffsetRequest.SmallestTimeString) {
              offsets += (tp -> earliestLeaderOffsets2)
            } else {
              offsets += (tp -> lastestLeaderOffsets2)
            }
          }
      }
      if (offsets.nonEmpty) { //将消费的已经过时的offset更新到当前kafka中最早的
        kc.setConsumerOffsets(groupId, offsets)
        println(s" update  $groupId offsets: $offsets")
      }
    } else {
      //没有消费过
      val leaderOffsets = if (reset == kafka.api.OffsetRequest.SmallestTimeString) {
        kc.getEarliestLeaderOffsets(partitions).right.get
      } else {
        kc.getLatestLeaderOffsets(partitions).right.get
      }
      val offsets = leaderOffsets.map {
        case (tp, lo) => (tp, lo.offset)
      }
      kc.setConsumerOffsets(groupId, offsets)
    }
  }

  def updateZKOffsets[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): Unit = {
    val groupId = kafkaParams("group.id")
    val offsetList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    var OffsetStr = ""
    for (offsets <- offsetList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
      OffsetStr += s"${offsets.partition}:${offsets.untilOffset},"
    }
    if (OffsetStr.length > 0) println(s"time:${parseDate()}, offset: ${OffsetStr.substring(0, OffsetStr.length - 1)}")
  }

  val sdf = new SimpleDateFormat("yyyy/MM/dd/HH/mm/ss")

  def parseDate(): String = {
    val strtmp = sdf.format(new Date(System.currentTimeMillis()))
    strtmp
  }
}
