package com.tmp

import java.text.NumberFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/1.
  */
object SexClass_2 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val Array(scoreLimit, minF, maxF) = args

        val middle = "/user/iflyrd/work/jjhu/sex/middle/20161129"
        val appInfo = "/user/iflyrd/work/jjhu/appInfo/"

        //(pkg, type)
        val info360RDD = sc.textFile(appInfo)
            .map(_.split("~")).filter(arr => arr.length >= 4 && arr(2).length > 0)
            .map(arr => (arr(0), arr(2)))
        info360RDD.cache()

        val appTypes = info360RDD.map(_._2).distinct().collect()
        val typeIndex = sc.parallelize(for (i <- appTypes.indices) yield (appTypes(i), i + 1))
        val appIndex = info360RDD.map(x => (x._2, x._1)).join(typeIndex)
            .map {
                case (tp, (pkg, index)) => (pkg, index)
            }

        val sexRDD = sc.textFile(middle)
            .map(_.split("~"))
            .filter(arr => arr.length >= 6 && arr(4).toInt > 0 && !arr(5).equals("UNKNOWN"))
            .map {
                case Array(imei, pkg, date, colDays, actDays, sex) =>
                    if (sex.equals("MALE")) {
                        (pkg, (imei, div(actDays.toInt, colDays.toInt), 0))
                    } else {
                        (pkg, (imei, div(actDays.toInt, colDays.toInt), 1))
                    }
            }

        val trainRDD = sexRDD.join(appIndex).map {
            case (pkg, ((imei, score, sex), index)) => ((imei, index), (score, sex))
        }
            .reduceByKey {
                case (x, y) => (x._1 + y._1, x._2)
            }
            .filter(x => x._2._1 <= scoreLimit.toDouble)
            .map {
                case ((imei, index), (score, sex)) => ((imei, sex), (index, divFormat(score, 4)))
            }
            .groupByKey()
            .filter(x => x._2.size >= minF.toInt && x._2.size <= maxF.toInt)
            .map {
                case ((imei, sex), iters) => {
                    val tmp = iters.toList.sortBy(_._1)
                    Seq(sex, tmp.map(x => Seq(x._1.toString, x._2).mkString(":")).mkString(" "))
                        .mkString(" ")
                }
            }

        trainRDD.coalesce(16).saveAsTextFile(s"/user/iflyrd/work/jjhu/sex/trainData_$scoreLimit-$minF-$maxF")
        info360RDD.unpersist()
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
