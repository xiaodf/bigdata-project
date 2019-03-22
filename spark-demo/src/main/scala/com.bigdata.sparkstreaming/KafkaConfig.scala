package com.bigdata.sparkstreaming

import java.util.Properties

object KafkaConfig {
  var zkQuorumnew = ""
  var brokerListnew =""

  def createKafkaConfigParam(groupId: String): Map[String, String] = {
    val kafkaParam = Map[String, String](
      "zookeeper.connect" -> zkQuorumnew,
      "metadata.broker.list" -> brokerListnew,
      "group.id" -> groupId,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString, //kafka.api.OffsetRequest.LargestTimeString ,SmallestTimeString
      "client.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    kafkaParam
  }

  def createKafkaConfigParam(groupId: String, zkQuorum: String, brokerList: String): Map[String, String] = {
    val kafkaParam = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString,
      "client.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    kafkaParam
  }

  def createKafkaConfigParamLm(groupId: String, zkQuorum: String, brokerList: String): Map[String, String] = {
    val kafkaParam = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
      "client.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    kafkaParam
  }

  def createSendKafkaConfigParam(brokerList: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")
    props
  }
}
