package com.rsion.spark.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @author : Rison 2021/6/3 上午8:38
 *获取kafka的工具类
 */
object KafkaUtil {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaParams = collection.mutable.Map(
    "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall0523_group",
    //latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)

  )

  /**
   * 创建DStream, 返回输入的数据，使用默认的消费组
   * @param ssc
   * @param topic
   * @return
   */
  def getKafkaStream(ssc: StreamingContext, topic: String) = {
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )
    dStream
  }

  /**
   * 创建DStream, 返回输入的数据，使用指定的的消费组
   * @param ssc
   * @param topic
   * @return
   */
  def getKafkaStream(ssc: StreamingContext, topic: String, groupId: String) = {
    kafkaParams.put("group.id", groupId)
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )
    dStream
  }

  /**
   * 创建DStream, 返回输入的数据，使用指定的的消费组, 从指定的偏移量读取
   * @param ssc
   * @param topic
   * @return
   */
  def getKafkaStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]) = {
    kafkaParams.put("group.id", groupId)
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams, offsets)
    )
    dStream
  }

}
