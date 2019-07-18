package com.atguigu.gmall0105.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.OrderInfo
import com.atguigu.gmall0105.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
  * kafka-->phoenix
  */
object OrderApp {
  //原始数据
//  {"payment_way":"1","delivery_address":"YugtBzSSKYLJnlpUEjkz","consignee":"NRXlpN",
  // "create_time":"2019-06-28 20:30:38","order_comment":"QEzIVCZowrtUsdokktmf",
  // "expire_time":"","order_status":"1","out_trade_no":"8884601532","tracking_no":"",
  // "total_amount":"956.0","user_id":"2","img_url":"","province_id":"6","consignee_tel":"13931492611",
  // "trade_body":"","id":"12","parent_order_id":"","operate_time":""}

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)
    val orderInfoDStream = inputDStream.map { record => {
      val orderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      //补充日期字段
      val dateTimeArr = orderInfo.create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      val timeArr = dateTimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)

      //电话脱敏
      val telTuple = orderInfo.consignee_tel.splitAt(7)
      orderInfo.consignee_tel = "*******" + telTuple._2

//      println(orderInfo)
      orderInfo
    }
    }

    //保存到hbase
    orderInfoDStream.foreachRDD{rdd=>

      rdd.saveToPhoenix("GMALL0105_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS",
          "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
          "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY",
          "CREATE_DATE", "CREATE_HOUR"),new Configuration,Some("hadoop102,hadoop103,hadoop104:2181") )

    }

    ssc.start()
    ssc.awaitTermination()

  }
}
