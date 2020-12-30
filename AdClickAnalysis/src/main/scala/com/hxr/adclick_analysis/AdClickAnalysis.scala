package com.hxr.adclick_analysis

/**
 * @Description:
 * @Author: CJ
 * @Data: 2020/12/30 15:51
 */
case class AdClickLog(userId:Long,adId:Long,province:String,city:String,timestamp:Long)

case class AdClickCountByProvince(windowEnd:String,province:String,count:Long)

class AdClickAnalysis {
  def main(args: Array[String]): Unit = {

  }
}
