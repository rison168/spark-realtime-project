package com.rsion.spark.realtime.application

import java.lang
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.rsion.spark.realtime.bean.DauInfo
import com.rsion.spark.realtime.util.{KafkaOffsetManagerUtil, KafkaUtil, MyESUtil, MyPropertiesUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @author : Rison 2021/6/3 上午10:36
 *         用户日活业务
 */
object DauApplication {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName.stripPrefix("$")).setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val prop: Properties = MyPropertiesUtil.load("config.properties")
    //定义消费主题、消费组
    val topic: String = prop.getProperty("kafka.topic")
    val groupId: String = prop.getProperty("kafka.groupId")

    //定义DStream
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    // 从redis 获取偏移量
    val offsetMap: Map[TopicPartition, Long] = KafkaOffsetManagerUtil.getOffsetToRedis(topic, groupId)
    //判断redis 是否包含该主题消费组的偏移量内容，比如第一次消费那一定是没有的
    if (offsetMap != null && offsetMap.size > 0) {
      //说明redis里面已经包含最新的一次消费偏移量，我们可以根据最新的消费偏移量进行消费
      recordDStream = KafkaUtil.getKafkaStream(ssc, topic, groupId, offsetMap)
    } else {
      //redis 没有保存到偏移量信息
      //默认按照kafka最新的位置开始消费 默认kafka每5秒会更新一次
      recordDStream = KafkaUtil.getKafkaStream(ssc, topic, groupId)
    }

    // 获取当前采集周期从kafka中消费的起始偏移量和结束偏移量，业务结束后保存到redis方便下次精准一次消费
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        // 因为recordDStream 底层封装的是kafkaRDD，混入了HasOffsetRanges特质，这个特质提供了获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    //TODO 业务操作

    //将json解析成json对象，同时时间拆分为日期和小时
    val jsonObjectDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        //json对象的时间
        val ts: lang.Long = jsonObject.getLong("ts")
        //将时间转换为格式话字符串
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        //切割为日期和小时
        val dateArray: Array[String] = dateStr.split(" ")
        val dt: String = dateArray(0)
        val hour: String = dateArray(1)
        jsonObject.put("dt", dt)
        jsonObject.put("hour", hour)
        jsonObject
      }
    }
    //通过redis, 对采集的数据进行一个去重操作
    val filterJsonObjectDStream: DStream[JSONObject] = jsonObjectDStream.mapPartitions {
      //以每个分区为单位，每个分区获取一个redis客户端连接
      jsonObjectItr => {
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        //定义一个集合，用于存放第一次登录的日志
        val listBuffer = new ListBuffer[JSONObject]()
        jsonObjectItr.foreach {
          jsonObject => {
            //获取日期
            val dt: String = jsonObject.getString("dt")
            //获取设备id
            val mid: String = jsonObject.getJSONObject("common").getString("mid")
            //拼接redis key
            val redisKey = "dau:" + dt
            //判断mid存在redis
            val isFirst: lang.Long = jedis.sadd(redisKey, mid)
            //设置失效key时间
            if (jedis.ttl(redisKey) < 0) {
              jedis.expire(redisKey, 3600 * 24)
            }
            if (isFirst == 1L) {
              //说明是第一次登录
              listBuffer.append(jsonObject)
            }
          }
        }
        jedis.close()
        listBuffer.toIterator
      }
    }
    //把第一次登录的保存到ES中
    filterJsonObjectDStream.foreachRDD{
      rdd => {
        //以分区为单位，数据批量保存草ES中
        rdd.foreachPartition{
          jsonObjectIterator => {
            val dauInfoList: List[(String, DauInfo)] = jsonObjectIterator.map {
              jsonObject => {
                val commonJsonObj: JSONObject = jsonObject.getJSONObject("common")
                val dauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObject.getString("dt"),
                  jsonObject.getString("hour"),
                  "00",
                  jsonObject.getLong("ts")
                )
                (dauInfo.mid, dauInfo)
              }
            }.toList

            //将数据保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList, "dau_info_" + dt)
          }
        }
        //数据处理完成（消费到es）后， 保存最新的偏移量到redis，方便下次精准一次消费
        KafkaOffsetManagerUtil.savaOffsetToRedis(topic, groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
