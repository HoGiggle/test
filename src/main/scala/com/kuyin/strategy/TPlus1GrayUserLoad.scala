package com.kuyin.strategy

import com.util.{HdfsConfHelper, LocalConfHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 酷音每天推荐纳入的用户
  * 1. 7天活跃天数 >= 2天 (使用audition下的用户行为作为活跃标准)
  * 2. 3天试听铃声数 >= 3 & < 500 (参数可配)
  *
  * Created by jjhu on 2017/3/23.
  */
object TPlus1GrayUserLoad {
  def main(args: Array[String]): Unit = {
    val Array(confPath, hdfsRange, cDate, audiStartDate, mode) = args
    val config = if (mode == "cluster") {
      HdfsConfHelper.getConfig(confPath).getConfig("job")
    } else {
      LocalConfHelper.getConfig(confPath).getConfig("job")
    }
    //初始化sc & 加载数据
    val conf = new SparkConf().setAppName(this.getClass.getName + "_" + cDate)
    val sc = new SparkContext(conf)
    val hdfsData = loadHDFSData(sc, config.getString("hdfs.inputPath"), hdfsRange)

    //周活跃天数过滤
    val weekActiveUser = weekActiveFilter(hdfsData, config.getInt("weekActiveDays"))

    //三天试听铃声数过滤
    val audiFilterUser = audiLimitFilter(hdfsData, audiStartDate, config.getInt("audiLevel"),
      config.getInt("audiFloorLimit"), config.getInt("audiUpperLimit"))

    val dayGrayUser = weekActiveUser.join(audiFilterUser)
      .map{
        case (uid, (dateCnt, audiCnt)) => Seq(cDate, uid).mkString("~")
      }

    //todo hbase维护灰度用户信息
    dayGrayUser.saveAsTextFile(Seq(config.getString("hdfs.outPath"), cDate).mkString("/"))

    sc.stop()
  }

  /**
    * 试听等频率过滤
    * @param data
    * @param audiStartDate 一段时间内的试听行为, 开始时间
    * @param audiLevel 目前只用到试听行为, 即行为权重 >= 1
    * @param floorLimit
    * @param upperLimit
    * @return
    */
  def audiLimitFilter(data: RDD[(String, String, String, String)],
                      audiStartDate: String,
                      audiLevel: Int,
                      floorLimit: Int,
                      upperLimit: Int): RDD[(String, Int)] = {
    data.filter {
      case (date, uid, itemId, level) => (date >= audiStartDate) && (level.toInt >= audiLevel)
    }
      .map{
        case (date, uid, itemId, level) => (uid, itemId)
      }
      .distinct()
      .map{
        case (uid, itemId) => (uid, 1)
      }
      .reduceByKey(_ + _)
      .filter{
        case (uid, cnt) => (cnt >= floorLimit) && (cnt < upperLimit)
      }
  }

  /**
    * 活跃天数过滤
    * @param data
    * @param weekActiveDays
    * @return
    */
  def weekActiveFilter(data: RDD[(String, String, String, String)],
                       weekActiveDays: Int): RDD[(String, Int)] = {
    data.map{
      case (date, uid, itemId, level) => (uid, date)
    }
      .distinct()
      .map{
        case (uid, date) => (uid, 1)
      }
      .reduceByKey(_ + _)
      .filter{
        case (uid, dateCnt) => dateCnt >= weekActiveDays
      }
  }

  /**
    * 加载HDFS上的数据, 试听数据
    *
    * @param sc
    * @param hdfsInput
    * @param hdfsRange
    * @return
    */
  def loadHDFSData(sc: SparkContext,
                   hdfsInput: String,
                   hdfsRange: String): RDD[(String, String, String, String)] = {
    val input = Array(hdfsInput, hdfsRange).mkString("/")
    sc.textFile(input)
      .map(_.split("~", -1))
      .filter(t => t.length >= 5)
      .map{
        case Array(date, uid, itemId, autoFlag, level) => (date, uid, itemId, level)
      }
  }
}
