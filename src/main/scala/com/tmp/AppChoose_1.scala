package com.tmp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/28.
  */
object AppChoose_1 {
    def main(args: Array[String]): Unit = {
        val appNameIn = "/user/iflyrd/work/jjhu/appInfo"
        val info360In = "/user/iflyrd/work/jjhu/item_table.txt"

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        sc.textFile(info360In).map(_.split("~"))
    }
}
