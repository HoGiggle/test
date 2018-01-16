package com.expriment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/21.
  */
object CacheTest {
    val EXP_DATA_PATH = """E:\movie.txt"""
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
        val sc = new SparkContext(conf)

        val rdd1 = sc.textFile(EXP_DATA_PATH)

        val rdd2 = rdd1.map(_.split("~")).filter(_.length >= 2)
        val rdd3 = rdd1.map(_.split("~")).filter(_.length >= 4)

        println(rdd2.count())
        println(rdd3.count())
        sc.stop()
    }
}
