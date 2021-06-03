package com.rsion.spark.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.slf4j.Logger
import redis.clients.jedis.Jedis

/**
 * @author : Rison 2021/6/3 上午9:43
 * kafka维护偏移量工具类
 */
object KafkaOffsetManagerUtil {
  /**
   * 从redis获取偏移量
   * @param topic
   * @param groupId
   * @return
   */
  def getOffsetToRedis(topic: String, groupId: String): Map[TopicPartition, Long] = {
    //获取redis客户端连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    val offsetKey: String = "offset:" + topic + ":" + groupId
    //获取当前消费者组消费的主题对应的分区以及偏移量
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭客户端
    jedis.close()

    //将Java的map转换为scala map
    import scala.collection.JavaConverters._
    val offsetsMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => (new TopicPartition(topic, partition.toInt), offset.toLong)
    }.toMap
    offsetsMap
  }

  /**
   * 将kafka偏移量保存到redis
   * @param topic
   * @param groupId
   * @param offsetRanges
   */
  def savaOffsetToRedis(topic: String, groupId: String, offsetRanges: Array[OffsetRange]) = {
    //redis key
    val offsetKey: String = "offset:" + topic + ":" + groupId
    val offsetMap = new util.HashMap[String, String]()
    //遍历offsetRanges, 将数据保存到map
    offsetRanges.foreach(
      offsetRange => {
        val partition: Int = offsetRange.partition
        val fromOffset: Long = offsetRange.fromOffset
        val untilOffset: Long = offsetRange.untilOffset
        offsetMap.put(partition.toString, untilOffset.toString)
      }
    )
    //连接redis客户端
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    jedis.hmset(offsetKey, offsetMap)
    //关闭客户端连接
    jedis.close()
  }

}
