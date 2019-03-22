package com.bigdata.sparkstreaming

/**
  * Created by xdf on 2018/11/23.
  */
object RedisTest {

  def main(args: Array[String]): Unit = {
    val redisHost = "10.10.86.26"
    val redisPort = 22016
    val redisPassword = "19e9791bfdce4e3683d57dc7bb7e0159"
    val dbIndex = 1
    val jedis = RedisClient.pool.getResource
    //jedis.select(dbIndex)
    println(jedis.sismember("userdid","zha"))
    //jedis.set("key1","key2")
    //println(jedis.get("key1"))

//        val redisClient = new Jedis(redisHost, redisPort)
//        redisClient.auth(redisPassword)
//    println(redisClient.sismember("userdid","00033c6d-044d-353b-8afe-46c6a5784de1"))
//    redisClient.set("key1","key1")
//    redisClient.set("key2","key2")
//    redisClient.set("key1","word")
    //redisClient.sadd("set1","set1")
//    val keys = Array("key1", "key2","set1")
//    for(key <- keys){
//      //println(redisClient.get(key))
//     // println(redisClient.exists(key))
//      println(redisClient.sismember("set1","set1"))
//
//    }

  }
}
