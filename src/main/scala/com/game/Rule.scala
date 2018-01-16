package com.game

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/4/7.
  */
object Rule {


  def main(args: Array[String]): Unit = {
    //前两天的浏览、点击行为
    val Array(dateRange) = args
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //product
    val pPath = "/user/iflyrd/work/jjhu/ccf_jd_v1/product/product_raw"
    val bProduct = sc.broadcast(sc.textFile(pPath).map(_.split("~", -1))
      .map(x => x(0).toInt).collect().toSet)



    val path = s"/user/iflyrd/work/jjhu/ccf_jd_v1/action/$dateRange"
    sc.textFile(path).map(_.split("~", -1))
      .map{
        case Array(uid, itemId, date, modelId, typeId, cate, brand) =>
          (uid.toInt, itemId.toInt, date, typeId.toInt, cate.toInt)
      }
      .filter{
        case (uid, itemId, date, actType, cate) =>
          (cate == 8) && ((actType == 1) || (actType == 6)) && bProduct.value.contains(itemId)
      }
      .map{
        case (uid, itemId, date, actType, cate) => ((uid, itemId), (date, 1))
      }
      .reduceByKey{
        case (x, y) => if (x._1 > y._1) (x._1, x._2 + y._2) else (y._1, x._2 + y._2)
      }
      .map{
        case ((uid, itemId), (date, cnt)) => (uid, (itemId, date, cnt))
      }
      .reduceByKey{
        case (x, y) => {
          if (x._2 > y._2) {
            x
          } else if (x._2 == y._2) {
            if (x._3 >= y._3) x else y
          } else {
            y
          }
        }
      }
      .map{
        case (uid, (itemId, date, cnt)) => Seq(uid, itemId).mkString(",")
      }
      .coalesce(1)
      .saveAsTextFile("/user/iflyrd/work/jjhu/tmp/ccf")

    sc.stop()
  }
}
