package com.kuyin.strategy

import com.util.CommonService
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/3/9.
  */
object Tmp {
  def main(args: Array[String]): Unit = {
    val Array(runDate) = args
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)


    val path = s"/user/iflyrd/kuyin/ctr/xgboost/predict/recommend/$runDate"
    val ctrRes = sc.textFile(path).map(_.split("~"))
      .filter(_.length == 3)
      .map{
        case Array(uid, itemId, score) => (uid, itemId, score.toDouble)
      }
      .sortBy(_._3, false)
      .map{
        case (uid, itemId, score) => Seq(uid, itemId, score).mkString("~")
      }
      .saveAsTextFile("/user/iflyrd/work/jjhu/kuyin/tmp/ctrSort")

    sc.stop()
  }

  def weekActiveRange(args: Array[String]) = {
    val Array(dateRange, today) = args
    val conf = new SparkConf().setAppName(this.getClass.getName + s"_$today")
    val sc = new SparkContext(conf)

    val path = s"/user/iflyrd/kuyin/input/audition/$dateRange"
    sc.textFile(path)
      .map(_.split("~", -1))
      .filter(t => t.length >= 5)
      .map{
        case Array(date, uid, itemId, autoFlag, level) => (uid, date)
      }
      .distinct()
      .map{
        case (uid, date) => (uid, 1)
      }
      .reduceByKey(_ + _)
      .map{
        case (uid, cnt) => (cnt, 1)
      }
      .reduceByKey(_ + _)
      .map{
        case (dateCnt, userCnt) => Seq(dateCnt, userCnt).mkString("~")
      }
      .sortBy(x => x)
      .coalesce(1)
      .saveAsTextFile(s"/user/iflyrd/work/jjhu/kuyin/tmp/weekActiveRange/$today")
    sc.stop()
  }

  def simRange_1(args: Array[String]) = {
    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //path
    val path = "/user/iflyrd/work/jjhu/kuyin/sim/"
    val data = sc.textFile(path, 300).map(_.split("~"))
      .map{
        case Array(uid1, uid2, sim) => {
          (CommonService.formatDouble(sim.toDouble, 2), 1)
        }
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .coalesce(1)
      .saveAsTextFile("/user/iflyrd/work/jjhu/kuyin/tmp/simRange")

    sc.stop()
  }

  def simRange(args: Array[String]) = {
    val Array(simLimit, topK) = args

    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //broadcast
    val bSimLimit = sc.broadcast(simLimit.toDouble)
    val bTopK = sc.broadcast(topK.toInt)
    //path
    val path = "/user/iflyrd/work/jjhu/kuyin/sim/"
    val data = sc.textFile(path).map(_.split(","))
      .map{
        case Array(uid1, uid2, sim) => {
          val simLen = sim.length
          val uid1Len = uid1.length
          (uid1.substring(1, uid1Len), uid2, sim.substring(0, simLen - 1).toDouble)
        }
      }
      .filter(_._3 > bSimLimit.value)
      .map{
        case (uid1, uid2, sim) => (uid1, 1)
      }
      .reduceByKey(_ + _)
      .cache()

    println("all user size = " + data.count())

    data.filter(_._2 >= bTopK.value)
      .map{
        case (uid, count) => (count, 1)
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .map{
        case (userSize, count) => Seq(userSize, count).mkString("~")
      }
      .saveAsTextFile("/user/iflyrd/work/jjhu/kuyin/tmp/simFilterRange")

    data.unpersist()
    sc.stop()
  }

  def audiUidRange(args: Array[String]){
    val Array(date) = args

    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //path
    val path = s"/user/iflyrd/kuyin/input/audition/$date"
    val uids = sc.textFile(path).map(_.split("~"))
      .map {
        case Array(day, uid, itemId, tp, value) => (uid, itemId)
      }
      .distinct()
      .map {
        case (uid, itemId) => (uid, 1)
      }
      .reduceByKey(_ + _)
      .map{
        case (uid, count) => (count, 1)
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .saveAsTextFile(s"/user/iflyrd/work/jjhu/kuyin/tmp/audiUidRange/$date")
    //    val bUids = sc.broadcast(uids.toMap)
    //
    //    val inPath = s"/user/iflyrd/kuyin/input/type_80000/$date"
    //
    //    //main run
    //    val data = ETLUtil.readMap(sc, inPath)
    //      .filter(m => bUids.value.contains(m.getOrElse(KYLSLogField.CUid, "")))
    //      .map(m => {
    //        val arr = new ArrayBuffer[String]()
    //        for (item <- m){
    //          arr += Seq(item._1, item._2).mkString(":")
    //        }
    //        Seq(bUids.value(m(KYLSLogField.CUid)), arr.mkString("~")).mkString("~")
    //      })
    //
    //    data.saveAsTextFile("/user/iflyrd/work/jjhu/kuyin/tmp/except")
    sc.stop()
  }

  def audiTimes(args: Array[String]): Unit = {
    val Array(date) = args

    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //path
    val path = s"/user/iflyrd/kuyin/input/audition/$date"
    sc.textFile(path).map(_.split("~"))
      .map {
        case Array(day, uid, itemId, value) => ((uid, itemId), value.toInt)
      }
      .reduceByKey(_ + _)
      .filter(_._2 >= 2)
      .sortBy(_._2, false)
      .map {
        case ((uid, itemId), value) => Seq(uid, itemId, value).mkString("~")
      }
      .saveAsTextFile("/user/iflyrd/work/jjhu/kuyin/tmp/audi")
    sc.stop()
  }
}
