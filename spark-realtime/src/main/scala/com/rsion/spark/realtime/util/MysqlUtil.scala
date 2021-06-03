package com.rsion.spark.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import java.util.Properties

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer



/**
 * @author : Rison 2021/6/3 上午10:09
 *mysql数据查询工具类
 */
object MysqlUtil {
  private val properties: Properties = MyPropertiesUtil.load("application.properties")
  private val driver = properties.getProperty("db.default.driver")
  private val url = properties.getProperty("db.default.url")
  private val user = properties.getProperty("db.default.user")
  private val password: String = properties.getProperty("db.default.password")

  /**
   * 查询
   * @param sql
   * @return
   */
  def queryList(sql: String): List[JSONObject] = {
    val resultList = new ListBuffer[JSONObject]
    //注册驱动
    Class.forName(driver)
    //建立连接
    val connection: Connection = DriverManager.getConnection(
      url,
      user,
      password
    )
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
    val resultSet: ResultSet = preparedStatement.executeQuery()
    val metaData: ResultSetMetaData = resultSet.getMetaData
    while (resultSet.next()){
      val jSONObject = new JSONObject()
      for (i <- 1 to metaData.getColumnCount){
        jSONObject.put(metaData.getColumnName(i), resultSet.getObject(i))
      }
      resultList.append(jSONObject)
    }
    resultSet.close()
    preparedStatement.close()
    connection.close()
    resultList.toList
  }
}
