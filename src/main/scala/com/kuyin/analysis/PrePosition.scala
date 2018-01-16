package com.kuyin.analysis

import com.kuyin.structure.KYLSLogField
import com.util.{CommonService, ETLUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats

/**
  * Created by jjhu on 2017/2/28.
  */

object PrePosition {
  def main(args: Array[String]): Unit = {
    val Array(dateRange, sDate) = args
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //推荐库itemId
//    val ringIdRDD = HBaseUtil.hbaseRead(sc, "iflyrd:KylsItemProfile")
//    val bRingId = sc.broadcast(ringIdRDD.map(_._1).collect().toSet)

    val path = s"/user/iflyrd/kuyin/input/type_80000/$dateRange"
    val data = ETLUtil.readMap(sc, path)
      .filter(m => {
        val evt = m.getOrElse(KYLSLogField.Evt, "")
        (m.getOrElse(KYLSLogField.OSId, "").toLowerCase == "android") &&
          (m.getOrElse(KYLSLogField.ObjType, "") == "10") &&
          ((evt == "2") || (evt == "63")) &&
          itemIdCheck(m.getOrElse(KYLSLogField.Obj, "")) &&
          (m.getOrElse(KYLSLogField.CUid, "").length > 0)
      }) //      .filter(m => bRingId.value.contains(m.getOrElse(KYLSLogField.Obj, ""))) //推荐库过滤
      .map(m => {
        val uid = m.getOrElse(KYLSLogField.CUid, "")
        val itemId = m.getOrElse(KYLSLogField.Obj, "")
        val evt = m.getOrElse(KYLSLogField.Evt, "")
        if (evt == "2") {
          (uid, itemId, 0)
        } else {
          (uid, itemId, 1)
        }
      }).cache()

    /** 试听用户量 **/
    data.map {
      case (uid, itemId, auto) => (uid, auto)
    }.distinct()
      .map {
        case (uid, auto) => (auto, 1)
      }
      .reduceByKey(_ + _).collect()
      .foreach {
        case (auto, count) => println(Seq("uidCount", auto, count).mkString("~"))
      }

    /** 试听铃声量 **/
    data.map {
      case (uid, itemId, auto) => (itemId, auto)
    }.distinct()
      .map {
        case (itemId, auto) => (auto, 1)
      }
      .reduceByKey(_ + _).collect()
      .foreach {
        case (auto, count) => println(Seq("itemIdCount", auto, count).mkString("~"))
      }

    /** 试听次数 0主动 1连续播放 **/
    data.map {
      case (uid, itemId, auto) => (auto, 1)
    }
      .reduceByKey(_ + _).collect()
      .foreach {
        case (auto, count) => println(Seq("audiCount", auto, count).mkString("~"))
      }
    println("all uids = " + data.map(_._1).distinct().count())
    println("all items = " + data.map(_._2).distinct().count())

    /** 试听铃声个数人次分布 **/
    val audiUidRange = data.distinct().map {
      case (uid, itemId, auto) => ((uid, auto), 1)  //人次、铃声次数分布
    }
      .reduceByKey(_ + _)
      .map {
        case ((uid, auto), count) => ((auto, count), 1)
      }
      .reduceByKey(_ + _).cache()
    audiUidRange.filter(_._1._1 == 0).map(x => (x._1._2, x._2)).sortBy(_._2).map(x => Seq(x._1, x._2).mkString("~"))
      .coalesce(1).saveAsTextFile(s"/user/iflyrd/work/jjhu/kuyin/tmp/audiUidRange/${sDate}_3days_self")
//    audiUidRange.filter(_._1._1 == 1).map(x => (x._1._2, x._2)).sortBy(_._2).map(x => Seq(x._1, x._2).mkString("~"))
//      .coalesce(1).saveAsTextFile(s"/user/iflyrd/work/jjhu/kuyin/tmp/audiUidRange/${sDate}_3days_auto")

//    data.map {
//      case (uid, itemId, auto) => (uid, itemId)
//    }
//      .distinct()
//      .map {
//        case (uid, itemId) => (uid, 1) //uid、itemId, 不同维度
//      }
//      .reduceByKey(_ + _)
//      .map {
//        case (uid, count) => (count, 1)
//      }
//      .reduceByKey(_ + _)
//      .sortBy(_._2)
//      .map(x => Seq(x._1, x._2).mkString("~"))
//      .coalesce(1)
//      .saveAsTextFile(s"/user/iflyrd/work/jjhu/kuyin/tmp/audiUidRange/${sDate}_3days_all")

    /** 铃声试听人次分布 **/
    val audiItemRange = data.distinct().map {
      case (uid, itemId, auto) => ((itemId, auto), 1)  //人次、铃声次数分布
    }
      .reduceByKey(_ + _)
      .map {
        case ((uid, auto), count) => ((auto, count), 1)
      }
      .reduceByKey(_ + _).cache()
    audiItemRange.filter(_._1._1 == 0).map(x => (x._1._2, x._2)).sortBy(_._2).map(x => Seq(x._1, x._2).mkString("~"))
      .coalesce(1).saveAsTextFile(s"/user/iflyrd/work/jjhu/kuyin/tmp/audiItemRange/${sDate}_3days_self")
//    audiItemRange.filter(_._1._1 == 1).map(x => (x._1._2, x._2)).sortBy(_._2).map(x => Seq(x._1, x._2).mkString("~"))
//      .coalesce(1).saveAsTextFile(s"/user/iflyrd/work/jjhu/kuyin/tmp/audiItemRange/${sDate}_3days_auto")
//
//    data.map {
//      case (uid, itemId, auto) => (uid, itemId)
//    }
//      .distinct()
//      .map {
//        case (uid, itemId) => (itemId, 1) //uid、itemId, 不同维度
//      }
//      .reduceByKey(_ + _)
//      .map {
//        case (uid, count) => (count, 1)
//      }
//      .reduceByKey(_ + _)
//      .sortBy(_._2)
//      .map(x => Seq(x._1, x._2).mkString("~"))
//      .coalesce(1)
//      .saveAsTextFile(s"/user/iflyrd/work/jjhu/kuyin/tmp/audiItemRange/${sDate}_3days_all")

    data.unpersist()
    sc.stop()
  }

  /** itemId filter
    *
    * @param itemId
    * @return
    */
  def itemIdCheck(itemId: String): Boolean = {
    val len = itemId.length
    ((len == 8) || (len == 14)) && (itemId.replaceAll("\\d", "").length == 0)
  }

  def tagsRange(args: Array[String]) = {
    val Array(date) = args
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //path
    val path = s"/user/iflyrd/kuyin/recommend/App/process/profile/item/$date"
    //        val path = "file:///E:\\data\\kuyin_item"

    //main
    val rdd = sc.textFile(path)
      .map(line => {
        val jValue = ETLUtil.json4sParse(line)
        implicit val formats = DefaultFormats
        val music = jValue.extract[Music]
        (music.ringId, music)
      })
      .filter {
        case (ringId, music) =>
          (ringId.length > 0) && (music.ringName.length > 0) && (music.singer.length > 0) //入库完整性过滤
      }
      .reduceByKey {
        case (x, y) => if (x.toSet >= y.toSet) x else y
      }
      .flatMap {
        case (ringId, music) => {
          val tagArr = music.tags.trim.split("\\|")
          tagArr.map(tag => (tag, 1))
        }
      }
      .reduceByKey(_ + _)
    //            .cache()

    //count & save
    rdd.sortBy(_._2, false)
      .map {
        case (tag, count) => Seq(tag, count).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("/user/iflyrd/work/jjhu/kuyin/tags/count")

    //index
    //        val tagsLocal = rdd.map{ case (tag, count) => tag}.collect()
    //        val tagsWithIndex = for (i <- 1 to tagsLocal.length) yield (tagsLocal(i), i)
    //        val tagsIndexRdd = sc.parallelize(tagsWithIndex)
    sc.stop()
  }

  def cuidAnalysis(args: Array[String]) = {
    val Array(date) = args
    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    // path setting
    val path = s"/user/iflyrd/kuyin/input/type_80000/$date"
    val mapRDD = ETLUtil.readMap(sc, path)
      .map {
        m => {
          val len = m.getOrElse("cuid", "").length

          if (len == 19) {
            (1, 0, 0) //normal exception null
          } else if (len > 0) {
            (0, 1, 0)
          } else {
            (0, 0, 1)
          }
        }
      }
      .reduce {
        case (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)
      }

    val sum = mapRDD._1 + mapRDD._2 + mapRDD._3
    val percent = CommonService.formatDouble(mapRDD._1 * 1.0d / sum, 2)
    println(Seq(date, mapRDD._1, mapRDD._2, mapRDD._3, percent).mkString("~"))

    sc.stop()
  }
}
