package com.tmp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/28.
  */
object AppChoose {
    def main(args: Array[String]): Unit = {
        val appNameIn = "/user/iflyrd/work/jjhu/AppName.txt"
        val pkgIn = "/user/iflyrd/work/jjhu/Apps.txt"
        val info360In = "/user/iflyrd/work/jjhu/item_table.txt"

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        //(pkg, appName)
        val info360RDD = sc.textFile(info360In).map(_.split("~")).filter(_.length >= 4)
            .map(arr => (arr(0).toLowerCase, arr(1).toLowerCase,
                arr(2).toLowerCase, arr(3).toLowerCase))
            .cache()
        val appNameRDD = sc.textFile(appNameIn).map(_.split("~")).filter(_.length >= 1)
            .map(arr => (arr(0).toLowerCase, 1))
        val pkgRDD = sc.textFile(pkgIn).map(_.split("~")).filter(_.length >= 1)
            .map(arr => (arr(0).toLowerCase, 1))

        val pkgJoin = info360RDD.map{
            case (pkg, appName, tp, tag) => (pkg, (appName, tp, tag))
        }.join(pkgRDD).map{
            case (pkg,((appName, tp, tag), tmp)) => (pkg, appName, tp, tag)
        }

        val appNameJoin = info360RDD.map{
                case (pkg, appName, tp, tag) => (appName, (pkg, tp, tag))
           }.join(appNameRDD)
            .map{
                case (appName,((pkg, tp, tag), tmp)) => (pkg, appName, tp, tag)
            }

        pkgJoin.union(appNameJoin).distinct().map{
            case (pkg, appName, tp, tag) => Seq(pkg, appName, tp, tag).mkString("~")
        }.coalesce(1)
            .saveAsTextFile("/user/iflyrd/work/jjhu/appInfo")

        info360RDD.unpersist()
        sc.stop()
    }
}
