package com.lb.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {

  private var factory:JestClientFactory = null
  def getClient:JestClient={
    if (factory==null) {
      build()
    }
    factory.getObject
  }

  def close(client: JestClient)={
    if (client!=null) {
      client.shutdownClient()
    }
  }

  private def build(): Unit ={
    factory=new JestClientFactory
    val esUrl = "master:9200"
    factory.setHttpClientConfig(new HttpClientConfig.Builder(esUrl).multiThreaded(true)
      .maxTotalConnection(20).
      connTimeout(10000).readTimeout(10000).build())
  }

  def executeIndexBulk(indexName:String,list:List[Any]): Unit ={
    val builder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")

    for (elem <- list) {
      val index: Index = new Index.Builder(elem).build()
      builder.addAction(index)
    }

    val client: JestClient = getClient
    val items: util.List[BulkResult#BulkResultItem] = client.execute(builder.build()).getItems
    close(client)
  }

  def main(args: Array[String]): Unit = {

  }

}
