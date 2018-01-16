package com.kuyin.etl

import com.kuyin.analysis.Music
import com.kuyin.structure.KYLSLogField
import com.util.{ETLUtil, HBaseUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats

/**
  * Created by jjhu on 2017/2/28.
  */
object Tmp {
  def main(args: Array[String]): Unit = {
    testUserDataLoad(args)
  }

  def itemCoverage(args: Array[String]) = {
    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //path
    val inPath = "/user/iflyrd/work/jjhu/kuyin/itemInfo/*"
    val ringIdRDD = HBaseUtil.hbaseRead(sc, "iflyrd:KylsItemProfile")
    val bRingId = sc.broadcast(ringIdRDD.map(_._1).collect().toSet)
    println("ringId num = " + bRingId.value.size)

    val result =sc.textFile(inPath).map(_.split(31.toChar.toString))
      .filter(_.length >= 8)
      .filter(x => bRingId.value.contains(x(0)) && (x(1) == "1"))
      .map{
        case Array(ringId, status, ringName, singer, tag, tmp1, tmp2, tmp3) => {
          var singerFlag = 0
          var tagFlag = 0
          var orFlag = 0
          var andFlag = 0

          if ((singer.length > 0) && (tag.length > 0)) andFlag = 1
          if (singer.length > 0) {
            singerFlag = 1
            orFlag = 1
          }
          if (tag.length > 0) {
            tagFlag = 1
            orFlag = 1
          }
          (1, tagFlag, singerFlag, orFlag, andFlag)
        }
      }
      .reduce{
        case (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5)
      }

    println(Seq(result).mkString("~"))

    sc.stop()
  }

  def testUserDataLoad(args: Array[String]) = {
    // Programmer arguments init:
    val Array(logDate, runDate) = args

    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //path
    val inPath = s"/user/iflyrd/kuyin/input/type_80000/$logDate"
    val outPath = s"/user/iflyrd/work/jjhu/kuyin/audi/$runDate"

    //main run
    val data = ETLUtil.readMap(sc, inPath)
      .filter(m => {
        val evt = m.getOrElse(KYLSLogField.Evt, "")
        (m.getOrElse(KYLSLogField.CUid, "") == "5000000000159456023") &&
          (m.getOrElse(KYLSLogField.OSId, "").toLowerCase == "android") &&
          (m.getOrElse(KYLSLogField.ObjType, "") == "10") &&
          ((evt == "2") || (evt == "33")) &&
          itemIdCheck(m.getOrElse(KYLSLogField.Obj, ""))
      })
      .map(m => {
        //(date, uid, itemId, autoFlag, level) level:行为分值, 后期点赞、分享、收藏行为会添加
        val uid = m.getOrElse(KYLSLogField.CUid, "")
        val itemId = m.getOrElse(KYLSLogField.Obj, "")
        val tp = m.getOrElse(KYLSLogField.TP, "1")
        val evt = m.getOrElse(KYLSLogField.Evt, "")
        if (evt == "2") { //试听
          Seq(runDate, uid, itemId, tp, "1").mkString("~")
        } else if (evt == "33"){ //展示
          Seq(runDate, uid, itemId, tp, "0").mkString("~")
        } else {KYLSLogField.STRING_VALUE_DEFAULT}
      })

    data.saveAsTextFile(outPath)
    sc.stop()
  }

  def itemPrint(args: Array[String]) = {
    val Array(date) = args
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //path setting
    val itemPath = s"/user/iflyrd/kuyin/recommend/App/process/profile/item/$date/"

    //day ring info load
    val dayRdd = sc.textFile(itemPath)
      .map(line => {
        val jValue = ETLUtil.json4sParse(line)
        implicit val formats = DefaultFormats
        val music = jValue.extract[Music]
        (music.ringId, music)
      })
      .filter {
        case (ringId, music) => !itemIdCheck(ringId)
      }
      .reduceByKey {
        case (x, y) => if (x.toSet >= y.toSet) x else y //多天重复
      }
      .map {
        case (ringId, music) => {
          val tagArr = music.tags
          Seq(ringId, music.ringName, music.singer, tagArr, music.toSet, music.toTry).mkString("~")
        }
      }
      .collect()
      .foreach(println)

    sc.stop()
  }

  def itemIdCheck(itemId: String): Boolean = {
    val len = itemId.length
    ((len == 8) || (len == 14)) && (itemId.replaceAll("\\d", "").length == 0)
  }
}
