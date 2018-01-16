package com.tmp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/30.
  */
object SexClass {
    def main(args: Array[String]): Unit = {
        val Array(useDate) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val sexData = "/user/iflyrd/work/zyzhang/gender_ypt/"
        val appData = s"/user/iflyms/inter/UseInfo/$useDate/"
        val middleData = s"/user/iflyrd/work/jjhu/sex/appName/$useDate"
        val info360Data = "/user/iflyrd/work/jjhu/appInfo/"

        //360 pkgName
        val bPkgData = sc.broadcast(sc.textFile(info360Data).map(_.split("~"))
            .filter(_.length >= 1).map(arr => (arr(0), arr(1))).collect().toMap)
        //(imei,sex)
        val sexRDD = sc.textFile(sexData).map(_.split("~"))
            .filter(arr => arr.length >= 2 && !arr(1).equals("UNKNOWN"))
            .map(arr => (arr(0), arr(1)))
        //(imei,(uid,collectDate,collectDays,pkg,useDays,appName))
        val appRDD = sc.textFile(appData).map(_.split("~"))
            .filter(arr => arr.length >= 8 && bPkgData.value.contains(arr(4).toLowerCase))
            .map(arr =>
                (arr(1), (arr(0), arr(2), arr(3), bPkgData.value(arr(4).toLowerCase), arr(6).count(ch => ch == '1')))
            )
            .filter(x => x._2._5 > 0)
        val middleRDD = sexRDD.join(appRDD).map{
            case (imei, (sex, (uid, collectDate, collectDays, appName, useDays))) => {
                Seq(uid, appName, collectDate, collectDays, useDays, sex).mkString("~")
            }
        }
        middleRDD.coalesce(64).saveAsTextFile(middleData)

        sc.stop()
    }
}
