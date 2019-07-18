package com.atguigu.gmall0105.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.rdd.RDD
import org.json4s.native.Serialization
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, streaming}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import collection.JavaConversions._

/**
  * 用户购买明细
  */
object SaleApp {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleApp")
    val ssc = new StreamingContext(sparkConf, streaming.Seconds(5))

    //2.从kafka中获取spark streaming
    val inputOrderDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    val inputDetailDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    val inputUserDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //3.转换为json对象
    val orderInfoDStream = inputOrderDStream.map { record =>
      val orderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      //4.补充日期字段，电话脱敏
      val datetimeArr = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)
      orderInfo.create_hour = datetimeArr(1).split(":")(0)

      val telTuple = orderInfo.consignee_tel.splitAt(7)
      orderInfo.consignee_tel = "*******" + telTuple._2

      orderInfo
    }
    val orderDetailInfoDStream = inputDetailDStream.map { record =>
      val orderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }

    //5.转换结构，提取id作为key
    val orderInfoWithKeyDStream = orderInfoDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailInfoWithKeyDStream = orderDetailInfoDStream.map(orderDetailInfo => (orderDetailInfo.order_id, orderDetailInfo))

    //6.全连接
    val fulljoinOrderDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDStream.fullOuterJoin(orderDetailInfoWithKeyDStream)

    //7.对两个数据流fulljoin的结果进行处理
    val saleDetailDStream: DStream[SaleDetail] = fulljoinOrderDStream.mapPartitions { joinIter =>
      //获取redis连接
      val client: Jedis = RedisUtil.getJedisClient

      //隐式转换，转换为密封类
      implicit val formats = org.json4s.DefaultFormats
      val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

      for ((orderid, (orderInfoOption, orderDetailInfoOption)) <- joinIter) {
        //理想状态，主表从表都有数据 =>把两个对象拼接成一个大对象
        if (orderInfoOption != None && orderDetailInfoOption != None) { //主表有，从表有
          //取值，封装对象，添加到集合中
          val orderInfo: OrderInfo = orderInfoOption.get
          val orderDetailInfo: OrderDetail = orderDetailInfoOption.get
          println("关联上了：" + orderInfo.id)
          val saleDetail = new SaleDetail(orderInfo, orderDetailInfo)
          saleDetailList += saleDetail

          //缓存主表，因为这只是一个批次的数据，无论关联成功与否，之后都可能还需要再次关联，所以主表必须缓存。
          val orderInfokey: String = "order_info" + orderInfo.id
          val orderInfoJson: String = Serialization.write(orderInfo)
          client.set(orderInfokey, orderInfoJson)



        } else if (orderInfoOption != None && orderDetailInfoOption == None) { //主表有，从表没有
          val orderInfo: OrderInfo = orderInfoOption.get
          println("主表有+++，从表没有---" + orderInfo.id)

          //来早了，来晚了，查询缓存判断来早还是来晚，查询从表
          val orderDetailkey: String = "order_detail" + orderInfo.id
          val orderDetailJsonSet: util.Set[String] = client.smembers(orderDetailkey)

          if (orderDetailJsonSet != null && orderDetailJsonSet.size() > 0) {
            println("主表"+ orderInfo.id+"来晚了，领走:"+orderDetailJsonSet.size+"个从表对象")
            for(orderDetailJson <- orderDetailJsonSet){
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson,classOf[OrderDetail])
              val saleDetail = new SaleDetail(orderInfo,orderDetail)
              saleDetailList += saleDetail
            }
            client.del(orderDetailkey)
          }else{
            println("主表" + orderInfo.id + "来早了")
          }

          //同理，缓存主表
          val orderInfokey: String = "order_info: " + orderInfo.id
          val orderInfoJson: String = Serialization.write(orderInfo)
          client.set(orderInfokey,orderInfoJson)




        } else { //主表没有，从表有
          val orderDetailInfo: OrderDetail = orderDetailInfoOption.get
          println("  主表没有----，从表有+++: order_id:" + orderDetailInfo.order_id +"||order_detail_id:"+orderDetailInfo.id)

          //从表判断来早还是来晚了，通过查询主表的缓存
          val orderInfokey: String = "order_info:" + orderDetailInfo.order_id
          val orderInfoJson: String = client.get(orderInfokey)
          if(orderInfoJson != null && orderInfoJson.length > 0){  //从表来晚
            println("从表来晚了 order_detail:"+orderDetailInfo.id+" orderId:"+orderDetailInfo.order_id)
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson,classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo,orderDetailInfo)
            saleDetailList += saleDetail
          }else{
            println("从表来早了 order_detail:"+orderDetailInfo.id+" orderId:"+orderDetailInfo.order_id)
            val orderDetailkey: String = "order_detail" + orderDetailInfo.order_id
            val orderDetailJson: String = Serialization.write(orderDetailInfo)
            client.sadd(orderDetailkey,orderDetailJson)

            //正式上线需要打开
//            client.expire(orderDetailkey,60)
          }
        }
      }

      client.close()
      saleDetailList.toIterator

    }

    //TODO
    //9.测试打印数据
    /*saleDetailDStream.foreachRDD(rdd => {
      println(rdd.collect().mkString("\n"))
    })*/

    //将结果保存到ES中
    val fullSaleDetailDstream: DStream[SaleDetail] = saleDetailDStream.mapPartitions { saleIter =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val userList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
      for (saleDetail <- saleIter) {

        val userInfoJson: String = jedis.hget("user_info", saleDetail.user_id)
        val userinfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        saleDetail.mergeUserInfo(userinfo)
        userList += saleDetail
      }
      jedis.close()
      userList.toIterator
    }
    fullSaleDetailDstream.foreachRDD{rdd=>
      val saleDetailList: List[SaleDetail] = rdd.collect().toList
      val saleDetailWithKeyList: List[(String, SaleDetail)] = saleDetailList.map(saleDetail=>(saleDetail.order_detail_id,saleDetail))
      MyEsUtil.insertBulk(GmallConstants.ES_INDEX_SALE_DETAIL,saleDetailWithKeyList)

    }

    // 把userInfo 保存到redis中
    inputUserDStream.map{record=>
      val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
      userInfo
    }.foreachRDD{rdd:RDD[UserInfo]=>
      val userList: List[UserInfo] = rdd.collect().toList
      val jedis: Jedis = RedisUtil.getJedisClient
      implicit val formats=org.json4s.DefaultFormats
      for (userInfo <- userList ) {   //  string  set list hash zset
        //设计user_info  redis  type  hash      key   user_info  , field   user_id  ,value user_info_json
        val userkey="user_info"
        val userJson: String = Serialization.write(userInfo)
        jedis.hset(userkey,userInfo.id,userJson)
      }
      jedis.close()
    }

    //10.循环检测采集数据
    ssc.start()
    ssc.awaitTermination()

  }

}
