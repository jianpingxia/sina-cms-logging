package com.sina.logging.cms

import java.text.SimpleDateFormat
import java.util.{Date, Locale}


import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._


/**
  * Created by James on 2017/5/20.
  */
object CMSFrontNginx {
  case class CMSFrontNginxObject(
                                  var idcName: String = "",
                                  var hostName: String = "",
                                  var apiDomain: String = "",
                                  var apiName: String = "",
                                  var status: Int = 0,
                                  var requestTime: Double = 0.0
                                ) extends Serializable

  def cleanRecords(records: InputDStream[(String, String)]): DStream[CMSFrontNginxObject] = {
    //{| NGINX[5816]: [07/Jun/2017:11:20:18 +0800]`-`"GET /newsapp/activities/msgbox.d.json?did=2cd0f01a09cc2f834a0cc121dc18191c&uid=&vn=621&act=get&from=6062193012 HTTP/1.1"`"-"`200`[10.13.216.57]`-`"-"`0.006`85`-`10.13.216.57`i.interface.sina.cn`-|10.13.3.44|web003.nwapcms.msina.bx.sinanode.com|"}
    records.map(line => {
      val tmp1 = line._2.split("`")
      val tmp2 = line._2.split("\\|")
      val result = new CMSFrontNginxObject()
      try {
        result.apiName = tmp1(2).split(" ")(1).split("\\?")(0) // "GET /newsapp/activities/msgbox.d.json?did=2cd0f01a09cc2f834a0cc121dc18191c&uid=&vn=621&act=get&from=6062193012 HTTP/1.1"
        result.status = tmp1(4).toInt
        result.requestTime = tmp1(8).toDouble
        result.apiDomain = tmp1(12)
        result.hostName = tmp2(tmp2.length - 2)
        val idcNameTemp = result.hostName.split("\\.") //web003.nwapcms.msina.bx.sinanode.com
        result.idcName = idcNameTemp(idcNameTemp.length - 3)
      } catch {
        case ex: Exception => /*println(line._2)*/
      }
      result
    }
    )
  }

  def computeByIDC(records: DStream[CMSFrontNginxObject],batchSize:String): Unit = {
    val idcTotalCount = records.map(record => record.idcName).countByValue()
    // DStream[(idcName,totalCount)]

    val idcStatusCount = records.map(record => (record.idcName, record.status)).countByValue()
    //DStream[((idcName,status),statusCount)]

    val idcStatusTotalRequestTime = records.map(record => ((record.idcName, record.status), record.requestTime)).reduceByKey(_ + _)
    //DStream[((idcName,status),totalRequestTime)]

    var result = idcStatusCount
      .join(idcStatusTotalRequestTime) //DStream[((idcName,status),(statusCount,totalRequestTime))]
      .map(line => (line._1._1, (line._1._2, line._2._1, line._2._2))) //DStream[(idcName,(status,statusCount,totalRequestTime))]
      .join(idcTotalCount) //DStream[(idcName,((status,statusCount,totalRequestTime),totalCount))]

    result.foreachRDD((rdd, time) => {
      val timestamp = new Date(time.milliseconds)
      rdd.map(line => {
        Map(
          "@timestamp" -> timestamp,
          "idcName" -> line._1,
          "totalCount" -> line._2._2,
          "status" -> line._2._1._1,
          "statusCount" -> line._2._1._2,
          "statusPercent" -> line._2._1._2 / line._2._2.toDouble,
          "avgRequestTime" -> line._2._1._3 / line._2._1._2
        )
      }).saveToEs(s"spark-sina-cms-front-nginx-byidc-${new SimpleDateFormat("yyyy.MM.dd").format(timestamp)}/${batchSize}s")
    }
    )
  }

  def computeByHOST(records: DStream[CMSFrontNginxObject],batchSize:String): Unit = {
    val hostTotalCount = records.map(record => record.hostName).countByValue()
    // DStream[(hostName,totalCount)]

    val hostStatusCount = records.map(record => (record.hostName, record.status)).countByValue()
    //DStream[((hostName,status),statusCount)]

    val hostStatusTotalRequestTime = records.map(record => ((record.hostName, record.status), record.requestTime)).reduceByKey(_ + _)
    //DStream[((hostName,status),totalRequestTime)]

    var result = hostStatusCount
      .join(hostStatusTotalRequestTime) //DStream[((hostName,status),(statusCount,totalRequestTime))]
      .map(line => (line._1._1, (line._1._2, line._2._1, line._2._2))) //DStream[(hostName,(status,statusCount,totalRequestTime))]
      .join(hostTotalCount) //DStream[(hostName,((status,statusCount,totalRequestTime),totalCount))]

    result.foreachRDD((rdd, time) => {
      val timestamp = new Date(time.milliseconds)
      rdd.map(line => {
        Map(
          "@timestamp" -> timestamp,
          "hostName" -> line._1,
          "totalCount" -> line._2._2,
          "status" -> line._2._1._1,
          "statusCount" -> line._2._1._2,
          "statusPercent" -> line._2._1._2 / line._2._2.toDouble,
          "avgRequestTime" -> line._2._1._3 / line._2._1._2
        )
      }).saveToEs(s"spark-sina-cms-front-nginx-byhost-${new SimpleDateFormat("yyyy.MM.dd").format(timestamp)}/${batchSize}s")
    }
    )
  }

  def computeByAPI(records: DStream[CMSFrontNginxObject],batchSize:String): Unit = {
    val apiTotalCount = records.map(record => record.apiDomain + record.apiName).countByValue()
    // DStream[(apiFullName,totalCount)]

    val apiStatusCount = records.map(record => (record.apiDomain + record.apiName, record.status)).countByValue()
    //DStream[((apiFullName,status),statusCount)]

    val apiStatusTotalRequestTime = records.map(record => ((record.apiDomain + record.apiName, record.status), record.requestTime)).reduceByKey(_ + _)
    //DStream[((apiFullName,status),totalRequestTime)]

    var result = apiStatusCount
      .join(apiStatusTotalRequestTime) //DStream[((apiFullName,status),(statusCount,totalRequestTime))]
      .map(line => (line._1._1, (line._1._2, line._2._1, line._2._2))) //DStream[(apiFullName,(status,statusCount,totalRequestTime))]
      .join(apiTotalCount) //DStream[(apiFullName,((status,statusCount,totalRequestTime),totalCount))]

    result.foreachRDD((rdd, time) => {
      val timestamp = new Date(time.milliseconds)
      rdd.map(line => {
        Map(
          "@timestamp" -> timestamp,
          "apiName" -> line._1,
          "totalCount" -> line._2._2,
          "status" -> line._2._1._1,
          "statusCount" -> line._2._1._2,
          "statusPercent" -> line._2._1._2 / line._2._2.toDouble,
          "avgRequestTime" -> line._2._1._3 / line._2._1._2
        )
      }).saveToEs(s"spark-sina-cms-front-nginx-byapi-${new SimpleDateFormat("yyyy.MM.dd").format(timestamp)}/${batchSize}s")
      //.foreach(println(_))
    }
    )
  }

  def doCompute(args: Array[String]): Unit = {
    //1、参数获取
    if (args.length < 6) {
      System.err.println(
        s"""
           |Usage: Command <batchSize> <appName> <kafkaBrokers> <kafkaTopics> <kafkaGroupId> <esNodes>
        """.stripMargin)
      System.exit(1)
    }
    val Array(batchSize, kafkaBrokers, kafkaTopics, kafkaGroupId, esNodes) = args

    //2、创建SSC
    val conf = new SparkConf()
    //conf.setAppName(appName)
    conf.set("es.nodes", esNodes)
    //conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(batchSize.toLong))

    //3、从kafka获取数据
    val topicsSet = kafkaTopics.split(",").toSet
    val kafkaParams =
      Map[String, String](
        "metadata.broker.list" -> kafkaBrokers,
        "group.id" -> kafkaGroupId
      )
    val records = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //4、执行计算&输出逻辑

    val cleanedRecords = cleanRecords(records).cache()
    computeByAPI(cleanedRecords,batchSize)
    computeByHOST(cleanedRecords,batchSize)
    computeByIDC(cleanedRecords,batchSize)

    //5、StreamingContext 启动与销毁
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
/*    val appName = "cms_front_nginx_log_compute"
    val kafkaBrokers = "10.13.88.190:9092,10.13.88.191:9092,10.13.88.192:9092"
    val kafkaTopics = "cms-front-nginx"
    val kafkaGroupId = "spark_receiver_cms_front_nginx"
    //val esNodes = "10.39.40.64:9220,10.39.40.93:9220,10.39.40.94:9220,10.39.40.95:9220,10.39.40.96:9220,10.39.40.97:9220"
    val esNodes = "10.211.103.202:9200,10.211.103.212:9200,10.211.103.222:9200"
    val batchSize = "30"
    doCompute(Array(batchSize, kafkaBrokers, kafkaTopics, kafkaGroupId, esNodes))*/
    doCompute(args)
  }
}

//spark-submit --name cms_front_nginx  --class com.sina.logging.cms.CMSFrontNginx --master spark://10.211.103.201:7077,10.211.103.211:7077,10.211.103.221:7077 --deploy-mode cluster --supervise --executor-memory 4G --total-executor-cores 3 http://10.211.103.201:8000/cms_front_nginx-1.0-SNAPSHOT.jar 30 10.13.88.190:9092,10.13.88.191:9092,10.13.88.192:9092 cms-front-nginx spark_receiver_cms_front_nginx 10.39.40.64:9220,10.39.40.93:9220,10.39.40.94:9220,10.39.40.95:9220,10.39.40.96:9220,10.39.40.97:9220
