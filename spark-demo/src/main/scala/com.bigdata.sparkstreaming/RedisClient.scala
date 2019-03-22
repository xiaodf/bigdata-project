package com.bigdata.sparkstreaming

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by xdf on 2018/11/26.
  */
object RedisClient extends Serializable{

  val redisTimeout = 30000
  val redisHost = "mb.y.redis.sohucs.com"
  val redisPort = 22621
  val redisPassword = "36a20fded94c4319a1986efbf3046ba3"

  val config = new GenericObjectPoolConfig()
  //设置最大连接数（100个足够用了，没必要设置太大）
  config.setMaxTotal(100)
  //最大空闲连接数
  config.setMaxIdle(10)
  //获取Jedis连接的最大等待时间（50秒）
  config.setMaxWaitMillis(50 * 1000)
  //在获取Jedis连接时，自动检验连接是否可用
  config.setTestOnBorrow(true)
  //在将连接放回池中前，自动检验连接是否有效
  config.setTestOnReturn(true)
  //自动测试池中的空闲连接是否都是可用连接
  config.setTestWhileIdle(true)
  //创建连接池

  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout,redisPassword)
  lazy val hook = new Thread {
    override def run = {
      //println("Execute hook thread: " + this)
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook.run)
}
