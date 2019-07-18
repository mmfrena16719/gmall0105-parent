package com.atguigu.gmall0105.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.{CouponAlertInfo, EventInfo}
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
  * 优惠券预警
  */
object EventApp {
  def main(args: Array[String]): Unit = {
    //构建上下文，从kafka中获取数据
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("EventApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val inputDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //转换格式为样例类
    val eventInfoDStream = inputDStream.map { record =>
      val jsonstr = record.value()
      val eventInfo = JSON.parseObject(jsonstr, classOf[EventInfo])
      eventInfo
    }

    //开窗口
    val eventInfoWindowDStream = eventInfoDStream.window(Seconds(20), Seconds(5))

    //统一设备分组
    val groupByMidDStream = eventInfoWindowDStream.map { eventinfo =>
      (eventinfo.mid, eventinfo)
    }.groupByKey()

    //判断预警
    //1.在一个设备内  2.三次及以上的领取优惠券（evid coupon) 且 uid 都不相同
    //3.没有浏览商品（evid clickItem)
    val checkCouponAlertDStream = groupByMidDStream.map {
      case (mid, eventInfoItr) =>
        val couponUidSet = new util.HashSet[String]()
        val itemIdsSet = new util.HashSet[String]()

        val eventIds = new util.ArrayList[String]()
        var notClickItem: Boolean = true
        breakable(
          for (eventInfo <- eventInfoItr) {
            eventIds.add(eventInfo.evid) // 用户行为
            if ("coupon".equals(eventInfo.evid)) {
              couponUidSet.add(eventInfo.uid) // 用户领券的uid
              itemIdsSet.add(eventInfo.itemid) // 用户领券的商品id
            } else if ("clickItem".equals(eventInfo.evid)) {
              notClickItem = false
              break()
            }
          }
        )

        //组合成元组，(标识是否达到预警要求，预警信息对象)
        (couponUidSet.size() >= 3 && notClickItem,
          CouponAlertInfo(mid, couponUidSet, itemIdsSet, eventIds, System.currentTimeMillis() )
        )

    }

    //过滤
    val filterDStream = checkCouponAlertDStream.filter(_._1)

    //增加一个id，用于保存到es的时候进行去重操作
    val alertInfoWithDStream = filterDStream.map { case (flag, alertInfo) =>
      val period = alertInfo.ts / 1000L / 60L
      val id = alertInfo.mid + "_" + period.toString
      (id, alertInfo)
    }

//    alertInfoWithDStream.foreachRDD{rdd =>
    alertInfoWithDStream.foreachRDD{rdd =>
//      println(rdd.collect().mkString("\n"))
      rdd.foreachPartition{alertResult =>
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_COUPON_ALERT,alertResult.toList)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
