package com.rsion.spark.realtime.bean

/**
 * @author : Rison 2021/6/3 上午11:48
 *封装日活数据的样例类
 */
case class DauInfo(  mid:String,//设备id
                     uid:String,//用户id
                     ar:String,//地区
                     ch:String,//渠道
                     vc:String,//版本
                     var dt:String,//日期
                     var hour:String,//小时
                     var mi:String,//分钟
                     ts:Long //时间戳
                  ) {}