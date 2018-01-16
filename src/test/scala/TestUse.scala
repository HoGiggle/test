package com.surus

import java.text.SimpleDateFormat

import com.kuyin.etl.{AuditionBehaviorLoad, FlumeLoad}
import com.kuyin.strategy.{DeliverStrategy, TopKSimUserFromAls}
import com.kuyin.structure.KYLSLogField
import com.util.{CommonService, JDHbaseColumn}
import junit.framework.TestCase

/**
  * Created by jjhu on 2017/2/24.
  */
object Color extends Enumeration{
  val Red = Value(2, "red")
  val Green = Value(3)
  val Blue = Value(5)
}

class TestUse extends TestCase {

  def testEnum() = {
    val color = Color.Red
    println(color.id)
    println(color.toString)
  }

  def addStr(data: Array[String]): Array[String] = {
    if (data.length <= 1) {
      return data
    }

    val result = new scala.collection.mutable.ArrayBuffer[String]
    result += data(0)


    val arr = new scala.collection.mutable.ArrayBuffer[String]
    for (i <- 1 until data.length) arr += data(i)
    result.toArray ++ addStr(arr.toArray)
  }

  def testLinkedList() = {
//    val singerSet = new scala.collection.mutable.HashSet[String]
//    val singerSet = new scala.collection.mutable.MutableList[String]
//    singerSet += "world"
//    val arr = Array("hello", "hello1")
//    singerSet ++= arr
//    for (item <- singerSet) println(item)
//    val arr = new scala.collection.mutable.ArrayBuffer[String]
    val arr = Array("1", "2", "3", "4","5", "6", "7", "8","9", "10", "11", "12")
    val map = Map("1" -> "周杰伦", "2" -> "周杰伦",
      "3" -> "周杰伦", "4" -> "路人2",
      "5" -> "路人3", "6" -> "路人4",
      "7" -> "路人5", "8" -> "路人6",
      "9" -> "路人7", "10" -> "路人8",
      "11" -> "路人9", "12" -> "路人10"
    )

    val res = DeliverStrategy.singleSinger(arr, map, 5, 5)
    for (item <- res) println(item)

  }

  def testUtils(): Unit = {
    val exceptSet = Set(KYLSLogField.SYS_Dur,
      KYLSLogField.SYS_RetDesc,
      KYLSLogField.SYS_ReqName,
      KYLSLogField.SYS_Ret,
      KYLSLogField.SYS_ResObj)
    val s = """{"type":"80000","time":"2017-04-05 07:33:11","tp":"0","loc":"推荐入口","locn":"推荐入口","locid":"","loctype":"","obj":"","objtype":"","evt":"1","seq":0,"ext":"","pos":"","acc":"","acctp":"1","ops":"1","apv":"1.0","mac":"121.41.22.107","ua":"Mozilla/5.0 (Linux; Android 5.1.1; Coolpad Y72-921 Build/LMY47V; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/47.0.2526.100 Mobile Safari/537.36","appid":"8320","scnid":"","cuid":"5hl33mp4c4xgipsv42fzaihi","proid":"01000001","cnid":"8320"}"""
    val res = FlumeLoad.etlStr2Map(s, exceptSet)

    for (item <- res) {
      println(item._1 + " -> " + item._2)
    }
  }

  def testTopK() = {
    val data: Array[(String, Double)] = Array(
      ("1", 1d),
      ("2", 2d),
      ("3", 4.5d),
      ("4", 2.3d),
      ("5", 7d),
      ("6", 0.4d),
      ("7", 1d),
      ("8", 2.6d),
      ("10", 2.4d),
      ("9", 10d)
    )

    val result = TopKSimUserFromAls.heapSort(data, 0, 4)
    for (item <- result) {
      println(item)
    }
  }

  def testMatches() = {
    val s1 = "Anesthesia旋律电音"
    val s2 = "56130271"
    val s3 = "20160100535079"

    println(AuditionBehaviorLoad.itemIdCheck(s1))
    println(AuditionBehaviorLoad.itemIdCheck(s2))
    println(AuditionBehaviorLoad.itemIdCheck(s3))
  }

  def testDateFormat() = {
    val date = "2016-02-02 23:17:44"
    val parseDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val minuteDF = new SimpleDateFormat("MMddHHm")
    try {
      val forDate = minuteDF.format(parseDF.parse(date))
      println(forDate)
      println(forDate.toInt)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def testActionFre() = {
    val sc = CommonService.scLocalInit(this.getClass.getName)
    val path = "file:///E:\\data\\ccf_jd\\action_0202"
    val parseDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val minuteDf = new SimpleDateFormat("MMddHHmm")
    val hourDf = new SimpleDateFormat("MMddHH")
    val DEFAULT_DATE = "02010000"

    val actionData = sc.textFile(path)
      .map(_.split("~", -1))
      .filter(_.length == 7)
      .filter(x => x(4) != "6")
      .map{
        case Array(uid, itemId, time, modelId, actType, cate, brand) => {
          try {
            val minTime = minuteDf.format(parseDf.parse(time))
            ((uid.toInt, itemId.toInt, minTime.toInt, modelId, actType.toInt, cate.toInt, brand.toInt), (time, 1))
          } catch {
            case e: Exception => {
              e.printStackTrace()
              ((uid.toInt, itemId.toInt, DEFAULT_DATE.toInt, modelId, actType.toInt, cate.toInt, brand.toInt), (time, 1))
            }
          }
        }
      }
      .reduceByKey{
        case (x, y) => (Seq(x._1, y._1).mkString("~"), x._2 + y._2)
      }
      .sortBy(x => x._2._2, false)
      .take(10)
      .foreach(println)

    sc.stop()
  }

  def testObject() = {
    val sc = CommonService.scLocalInit(this.getClass.getName)
    val bJDHbaseCol = sc.broadcast(JDHbaseColumn)

    val arr = Array(1, 2, 3, 4, 5)
    val rdd = sc.parallelize(arr)
    rdd.map(x => Seq(bJDHbaseCol.value.P_AddCart_Rate, x).mkString("~"))
      .collect().foreach(println)

    sc.stop()
  }

  def testIntVSStr() = {
    var i = 0l
    val limit = 1000000000l

    val start = System.currentTimeMillis()
    while (i < limit) {
      if (1 == 1) i += 1
    }
    val end = System.currentTimeMillis()

    i = 0l
    val start1 = System.currentTimeMillis()
    while (i < limit) {
      if ("1" == "1") i += 1
    }
    val end1 = System.currentTimeMillis()

    println("Int cost = " + (end - start))
    println("String cost = " + (end1 - start1))
  }
}
