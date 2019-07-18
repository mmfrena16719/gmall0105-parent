package com.atguigu.gmall0105.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.StartupLog
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
  * 日活实时kafka-->redis-->phonenix
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //传递kafka的消费主题和的spark的上下文获取sparkStreaming
    val inputDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    /*inputDStream.foreachRDD(rdd => {
      println(rdd.map(_.value()).collect().mkString("\n"))
    })*/
//    {"area":"shandong","uid":"246","os":"andriod","ch":"360","appid":"gmall0105",
    // "mid":"mid_97","type":"startup","vs":"1.1.3","ts":1561588513757}

    //1.数据流转换，结构变成case class，补充两个时间字段
    val startuplogDStream = inputDStream.map { record => {
      val jsonStr = record.value()
      val startupLog = JSON.parseObject(jsonStr, classOf[StartupLog])

      val dateTimeStr = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))
      val dateArr = dateTimeStr.split(" ")
      startupLog.logDate = dateArr(0)
      startupLog.logHour = dateArr(1)
//      println(startupLog)
//      StartupLog(mid_172,366,gmall0105,heilongjiang,ios,appstore,null,1.2.0,2019-06-27,16,1561588839320)
      startupLog
    }
    }

    startuplogDStream.cache()

    //2.利用用户清单进行过滤去重，只保留清单中不存在的用户访问记录
//    反复查询redis性能有消耗，而transform会周期性的执行driver中的代码
    val filterDStream = startuplogDStream.transform { rdd =>
      val client = RedisUtil.getJedisClient
      val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

      val key = "dau" + dateStr
      val dauMidSet = client.smembers(key)
      client.close()

      // 清单具体执行实在excutor中，所以使用广播变量
      val dauMidBroadcast = ssc.sparkContext.broadcast(dauMidSet)
      println("过滤前" + rdd.count())

      val filterRDD = rdd.filter { log =>
        val dauMidValue = dauMidBroadcast.value
        !dauMidValue.contains(log.mid)
      }
      println("过滤后" + filterRDD.count())
      filterRDD
    }

    filterDStream.print()

    //3.批次内进行去重:mid分组，每组取第一个值
    val groupByKeyDStream = filterDStream.map(startuplog => (startuplog.mid,startuplog)).groupByKey()
    val distictDStream = groupByKeyDStream.flatMap{
      case(mid,startupLogList) => {
        startupLogList.toList.take(1)
      }
    }

    //4.保留今日访问过的数据
    distictDStream.foreachRDD(rdd =>
      rdd.foreachPartition(startupLogList => {
        val client = RedisUtil.getJedisClient
        for(startuplog <- startupLogList){
          val key = "dau" + startuplog.logDate
          client.sadd(key,startuplog.mid)
          println(startuplog)
        }
        client.close()
      }))

    //5.把数据写入hbase+phoenix
    distictDStream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0105_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
