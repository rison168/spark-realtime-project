package com.rsion.spark.realtime.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * @author : Rison 2021/6/3 上午8:23
 *读取配置文件工具类
 */
object MyPropertiesUtil {

  def load(propertiesName: String): Properties = {
    val properties = new Properties()
    //加载指定配置文件
    properties.load(
    new InputStreamReader(
    Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
    StandardCharsets.UTF_8
    )
    )
    properties
  }

  def main(args: Array[String]): Unit = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    println(prop.getProperty("kafka.broker.list"))
  }
}
