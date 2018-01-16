package com.kuyin.strategy

import com.kuyin.structure.RecommendALG
import com.util.HBaseUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by jjhu on 2017/4/17.
  */
trait RecommendUtils extends Serializable {

  def isIncluded(alg: Int,
                 hisValue: Int): Boolean = {
    if ((hisValue % alg) == 0) true else false
  }


  def saveRecommHBASE(sc: SparkContext,
                      hbaseTable: String,
                      zookeeperQuorum: String,
                      hbaseColumn: String,
                      result: RDD[(String, String)],
                      date: String,
                      alg: RecommendALG.Value) = {
    HBaseUtil.setHBaseConf(zookeeperQuorum)
    val startRow = s"$date~"
    val stopRow = s"$date~~"
    val optString = s"-gte $startRow -lte $stopRow"
    val historyRecomm = HBaseUtil.hbaseRead(sc, hbaseTable, s"cf:$hbaseColumn", optString)
      .map(t => (t._1, t._2.getOrElse(s"cf:$hbaseColumn", "1").toInt))

    val hbaseRes = result.map {
      case (uid, itemId) => (Seq(date, uid, itemId).mkString("~"), alg.id)
    }
      .leftOuterJoin(historyRecomm)
      .map {
        case (key, (algFlag, opt)) => {
          var value = 1
          if (opt.isEmpty) {
            value = algFlag
          } else {
            val hisFlag = opt.get
            if (isIncluded(algFlag, hisFlag)) {
              value = hisFlag
            } else {
              value = hisFlag * algFlag
            }
          }
          (key, Map(s"cf:$hbaseColumn" -> value.toString))
        }
      }
    HBaseUtil.hbaseWrite(hbaseTable, hbaseRes)
  }

  /**
    * 加载HBase中的数据
    *
    * @param sc
    * @param hbaseTable
    * @param zookeeperQuorum
    * @param startRow
    * @param stopRow
    * @param hbaseColumn
    * @return
    */
  def loadHBaseData(sc: SparkContext,
                    hbaseTable: String,
                    zookeeperQuorum: String,
                    startRow: String,
                    stopRow: String,
                    hbaseColumn: String): RDD[(String, Int)] = {
    HBaseUtil.setHBaseConf(zookeeperQuorum)
    val optString = s"-gte $startRow -lte $stopRow"
    HBaseUtil.hbaseRead(sc, hbaseTable, s"cf:$hbaseColumn", optString)
      .map(t => (t._1, t._2.getOrElse(s"cf:$hbaseColumn", "1").toInt))
  }

  /**
    * 存储推荐算法召回结果
    *
    * @param result
    * @param resultOutput
    * @param cDate
    * @param resultOutputPartition
    * @param algFlag 算法标志
    */
  def saveRecommHDFSWithScore(result: RDD[(String, String, String)],
                              resultOutput: String,
                              cDate: String,
                              resultOutputPartition: Int,
                              algFlag: String): Unit = {
    val output = Array(resultOutput, cDate, algFlag).mkString("/")
    result.map {
      case (uid, nid, score) => Seq(uid, nid, score).mkString("~")
    }
      .coalesce(resultOutputPartition, true).saveAsTextFile(output)
  }

  /**
    * 存储推荐算法召回结果
    *
    * @param result
    * @param resultOutput
    * @param cDate
    * @param resultOutputPartition
    * @param algFlag 算法标志
    */
  def saveRecommHDFS(result: RDD[(String, String)],
                     resultOutput: String,
                     cDate: String,
                     resultOutputPartition: Int,
                     algFlag: String): Unit = {
    val output = Array(resultOutput, cDate, algFlag).mkString("/")
    result.map {
      case (uid, nid) => Seq(uid, nid).mkString("~")
    }
      .coalesce(resultOutputPartition, true).saveAsTextFile(output)
  }

  /**
    * 加载每天的灰度用户
    *
    * @param sc
    * @param grayPath
    * @param date
    * @return
    */
  def loadGrayUser(sc: SparkContext,
                   grayPath: String,
                   date: String): RDD[(String, String)] = {
    sc.textFile(Seq(grayPath, date).mkString("/")).map(_.split("~", -1))
      .map {
        case Array(grayDate, uid) => (uid, grayDate)
      }
  }
}
