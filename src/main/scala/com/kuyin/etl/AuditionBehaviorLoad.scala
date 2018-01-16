package com.kuyin.etl

import com.kuyin.structure.{BehaviorLevel, KYLSLogField}
import com.util.ETLUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 酷音试听行为加载:
  * 1.android用户的试听行为
  * 2.CUid itemId 完整、可用
  * 3.包含(date, uid, itemId, autoFlag, level)
  */
object AuditionBehaviorLoad {
  def main(args: Array[String]): Unit = {
    // Programmer arguments init:
    val Array(logDate, partition) = args

    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //path
    val inPath = s"/user/iflyrd/kuyin/input/type_80000/$logDate"
    val outPath = s"/user/iflyrd/kuyin/input/audition/$logDate"

    //main run
    val data = ETLUtil.readMap(sc, inPath)
      .filter(m => {
        val evt = m.getOrElse(KYLSLogField.Evt, "")
        (m.getOrElse(KYLSLogField.OSId, "").toLowerCase == "android") &&
          (m.getOrElse(KYLSLogField.ObjType, "") == "10") &&
          ((evt == "2") || (evt == "33")) &&
          itemIdCheck(m.getOrElse(KYLSLogField.Obj, "")) &&
          (m.getOrElse(KYLSLogField.CUid, "").length > 0)
      })
      .map(m => {
        //(date, uid, itemId, autoFlag, level) level:行为分值, 后期点赞、分享、收藏行为会添加
        val uid = m.getOrElse(KYLSLogField.CUid, "")
        val itemId = m.getOrElse(KYLSLogField.Obj, "")
        val tp = m.getOrElse(KYLSLogField.TP, "1")  //默认为系统反馈
        val evt = m.getOrElse(KYLSLogField.Evt, "")
        if (evt == "2") { //试听
          Seq(logDate, uid, itemId, tp, BehaviorLevel.Audition).mkString("~")
        } else if (evt == "33"){ //展示
          Seq(logDate, uid, itemId, tp, BehaviorLevel.Display).mkString("~")
        } else {KYLSLogField.STRING_VALUE_DEFAULT} //其他行为拓展块
      })
      .filter(_.length > 0)

    //repartition and save in hdfs
    data.coalesce(partition.toInt).saveAsTextFile(outPath)
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
}
