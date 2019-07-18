package com.atguigu.gmall0105.realtime.util

import java.util
import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}
import collection.JavaConversions._

/**
  *
  */
object MyEsUtil {

  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  //获取客户端
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  //关闭客户端
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client))
      try
        client.shutdownClient()
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
  }

  //建立连接
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20).connTimeout(10000).readTimeout(10000).build())
  }

  //批量数据插入到ES
  def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {
    if (docList.size > 0) {
      val client = getClient
      val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
      for ((id, doc) <- docList) {
        val indexBuilder = new Index.Builder(doc)
        if (id != null) {
          indexBuilder.id(id)
        }
        val index = indexBuilder.build()
        bulkBuilder.addAction(index)
      }

      val bulk = bulkBuilder.build()
      var items: util.List[BulkResult#BulkResultItem] = null
      try {
        items = client.execute(bulkBuilder.build()).getItems
      } catch {
        case ex: Exception => println(ex.toString)
      } finally {
        close(client)
        println("保存" + items.size() + "条数据")
        for (item <- items) {
          if (item.error != null && item.error.nonEmpty){
            println(item.error)
            println(item.errorReason)
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val client = getClient

    val index = new Index.Builder(Stud("zhang3", "zhang33")).index("gmall0105_stud")
      .`type`("_doc").id("stu123").build()

    client.execute(index)
  }

  case class Stud(name:String ,nickname:String){

  }

}
