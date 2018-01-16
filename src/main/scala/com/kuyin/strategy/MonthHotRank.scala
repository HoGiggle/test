package com.kuyin.strategy

import com.util.{HBaseUtil, HdfsConfHelper, LocalConfHelper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 兜底库:
  * 使用铃声月试听人次作为度量
  * 1.试听行为已经etl
  * 2.试听人次相对试听次数定义兜底库更准确
  */
object MonthHotRank {
  def main(args: Array[String]): Unit = {
    // Programmer arguments init:
    val Array(confPath, mode, logDate) = args
    val config = if (mode == "cluster") {
      HdfsConfHelper.getConfig(confPath).getConfig("job")
    } else {
      LocalConfHelper.getConfig(confPath).getConfig("job")
    }

    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //推荐库加载
    val bRecData = sc.broadcast(HBaseUtil.hbaseRead(sc, config.getString("hbase.recTable"))
      .map(x => x._1).collect().toSet)

    //热榜生成 & join 推荐库
    val path = s"${config.getString("hdfs.behaviorPath")}/$logDate"
    val topNData = sc.textFile(path).map(_.split("~", -1))
      .filter(_.length == 5)
      .map{
        case Array(date, uid, itemId, auto, behaviorFlag) => (date, uid, itemId, behaviorFlag.toInt)
      }
      .filter(_._4 == 1)
      .map{
        case (date, uid, itemId, behaviorFlag) => ((date, uid, itemId), 1)
      }
      .reduceByKey{
        case (x, y) => x
      }
      .map{
        case ((date, uid, itemId), tmp) => (itemId, 1)
      }
      .reduceByKey(_ + _)
      .filter(x => bRecData.value.contains(x._1))
      .sortBy(_._2, false)
      .map{
        case (itemId, cnt) => itemId
      }
      .take(config.getInt("topN"))

    //save in hbase
    val localData = for (index <- topNData.indices) yield (topNData(index), index + 1)
    val bColumn = sc.broadcast(config.getString("hbase.rankCol"))
    val hbaseRes = sc.parallelize(localData)
      .map{
        case (itemId, rank) => (itemId, Map(bColumn.value -> rank.toString))
      }
    HBaseUtil.hbaseWrite(config.getString("hbase.hotTable"), hbaseRes)

    sc.stop()
  }
}
