package com.tmp

import java.text.NumberFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/30.
  */
object SexClass_1 {
    def main(args: Array[String]): Unit = {
        val Array(useDate) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val middleData = s"/user/iflyrd/work/jjhu/sex/middle/$useDate"
        val info360Data = "/user/iflyrd/work/jjhu/appInfo/"

        //(pkg, (appName, tags))
        val info360RDD = sc.textFile(info360Data)
            .map(_.split("~")).filter(arr => arr.length >= 4 && arr(2).length > 0)
            .map(arr => (arr(0), arr(1)))

        //(pkg,(uid, colDays, actDays, sex))
        val trainRDD = sc.textFile(middleData)
            .map(_.split("~")).filter(arr => arr.length >= 6 && arr(4).toInt > 0 && !arr(5).equals("UNKNOWN"))
            .map(arr => (arr(1),(arr(0),arr(2),arr(3),arr(4),arr(5))))

        info360RDD.join(trainRDD).map{
            case (pkg, (appName, (uid, date, colDays, actDays, sex))) =>
                Seq(uid, appName, date, colDays, actDays, sex).mkString("~")
        }.coalesce(64).saveAsTextFile(s"/user/iflyrd/work/jjhu/sex/appName/$useDate")
    }

    def div(clickPv: Int,
            displayPv: Int,
            decimalBit: Int): Double = {
        val nFormat = NumberFormat.getNumberInstance
        var clickRate = 0d
        nFormat.setMaximumFractionDigits(decimalBit)

        if (displayPv > 0) {
            clickRate = nFormat.format(clickPv * 1.0d / displayPv).toDouble
        }
        clickRate
    }
}
