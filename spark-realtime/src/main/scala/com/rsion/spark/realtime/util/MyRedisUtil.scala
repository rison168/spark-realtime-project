package com.rsion.spark.realtime.util

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author : Rison 2021/6/3 上午8:17
 *获取Jedis客户端工具类
 */
object MyRedisUtil {

  //定义一个连接池对象
  private var jedisPool: JedisPool = null;

  //创建JedisPool连接池对象
  def build() = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val host: String = properties.getProperty("redis.host")
    val port: String = properties.getProperty("redis.port")

    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌是否等待
    jedisPoolConfig.setMaxWaitMillis(5000) //最大等待时间
    jedisPoolConfig.setTestOnBorrow(true) //每次连接进行测试
    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
  }

  def getJedisClient(): Jedis = {
    if(jedisPool ==  null){
      build()
    }
    jedisPool.getResource
  }


}
