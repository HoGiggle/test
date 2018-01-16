package com.tmp

import java.text.NumberFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/2.
  */
object SexAppRange {
    def main(args: Array[String]): Unit = {
        val Array(useDate) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val in = s"/user/iflyrd/work/jjhu/sex/appName/$useDate"
        //uid, pkg, appName, colDays, actDays, sex
        val tmp = sc.textFile(in).map(_.split("~")).map{
            case Array(uid, appName, date, colDays, actDays, sex) =>
                ((uid, appName), date)
        }.reduceByKey{
            case (x, y) => if (x > y) x else y
        }.map(x => (x._1._1, x._1._2)).cache()

        //性别appName topK分布
//        val male = tmp.filter(x => x(5) == "MALE").cache()
////        male.map(x => (x(2), 1)).reduceByKey(_ + _).sortBy(_._2, false)
////            .map(x => Seq(x._1, x._2).mkString("~"))
////            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/maleAppCnt")
////        male.map(x => (x(0), x(2))).distinct()
////            .map(x => (x._1, 1)).reduceByKey(_ + _).map(x => (x._2, 1))
////            .reduceByKey(_ + _).sortByKey()
////            .map(x => Seq(x._1, x._2).mkString("~"))
////            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/maleAppPsCnt")
//        male.map(x => (x(2), (1, div(x(4).toInt, x(3).toInt))))
//            .reduceByKey{
//                case (x, y) => (x._1 + y._1, x._2 + y._2)
//            }
//            .map(x => (x._1, x._2._1, divFormat(x._2._2 / x._2._1, 4).toFloat))
//            .sortBy(_._3, false)
//            .map(x => Seq(x._1, x._2, x._3).mkString("~"))
//            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/maleAppScore")


        tmp.map(x => (x._2, 1)).reduceByKey(_ + _).sortBy(_._2, false)
            .map(x => Seq(x._1, x._2).mkString("~"))
            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/appCnt")
        tmp.map(x => (x._1, 1)).reduceByKey(_ + _).map(x => (x._2, 1))
            .reduceByKey(_ + _).sortByKey()
            .map(x => Seq(x._1, x._2).mkString("~"))
            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/appPsCnt")
//        female.map(x => (x(2), (1, div(x(4).toInt, x(3).toInt))))
//            .reduceByKey{
//                case (x, y) => (x._1 + y._1, x._2 + y._2)
//            }
//            .map(x => (x._1, x._2._1, divFormat(x._2._2 / x._2._1, 4).toFloat))
//            .sortBy(_._3, false)
//            .map(x => Seq(x._1, x._2, x._3).mkString("~"))
//            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/femaleAppScore")
        //性别活跃天数分布

//        male.unpersist()
        tmp.unpersist()
        sc.stop()
    }

    def div(x: Int, y: Int): Double = {
        var res = 0.0d
        if (y != 0) {
            res = x * 1.0d / y
        }
        res
    }

    def divFormat(in: Double,
                  decimalBit: Int): String = {
        val nFormat = NumberFormat.getNumberInstance
        nFormat.setMaximumFractionDigits(decimalBit)
        nFormat.format(in)
    }
}
