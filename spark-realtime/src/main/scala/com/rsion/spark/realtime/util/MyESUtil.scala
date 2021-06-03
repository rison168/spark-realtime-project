package com.rsion.spark.realtime.util

import java.util.Properties
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

/**
 * @author : Rison 2021/6/3 上午11:44
 *         操作ES客户端工具类
 */
object MyESUtil {
  private var jestFactory: JestClientFactory = null

  def build() = {
    val properties: Properties = MyPropertiesUtil.load("application.properties")
    val serverUrl: String = properties.getProperty("elasticsearch.server.url")
    jestFactory = new JestClientFactory()
    jestFactory.setHttpClientConfig(
      new HttpClientConfig.Builder(serverUrl)
        .multiThreaded(true)
        .maxTotalConnection(20)
        .connTimeout(10000)
        .readTimeout(1000).build()
    )
  }

  /**
   * 获取es客户端
   *
   * @return
   */
  def getJestClient(): JestClient = {
    if (jestFactory == null) {
      build()
    }
    jestFactory.getObject
  }

  /**
   * 向ES中批量插入数据
   *
   * @param infoList
   * @param indexName
   */
  def bulkInsert(infoList: List[(String, Any)], indexName: String): Unit = {
    if (infoList != null && infoList.size > 0) {
      //获取ES客户端
      val jestClient: JestClient = getJestClient()
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
      infoList.foreach {
        case (id, info) => {
          val index: Index = new Index.Builder(info)
            .index(indexName)
            .id(id)
            .`type`("_doc")
            .build()
          bulkBuilder.addAction(index)
        }
      }
      //创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult: BulkResult = jestClient.execute(bulk)
      jestClient.close()
    }
  }

}
