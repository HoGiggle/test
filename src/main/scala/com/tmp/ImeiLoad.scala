package com.tmp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/29.
  */
object ImeiLoad {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val raw = sc.newAPIHadoopFile("/user/iflyol/ossp/inter/EtlSys/WeekActiveUser/20161121/",
            classOf[com.hadoop.mapreduce.LzoTextInputFormat],
            classOf[org.apache.hadoop.io.LongWritable],
            classOf[org.apache.hadoop.io.Text])
            .map(_._2.toString)

        raw.map(_.split("~")).filter(_.length >= 4)
            .filter(arr => arr(3).length > 0)
            .map(arr => arr(3))
            .distinct()
            .saveAsTextFile("/user/iflyrd/work/jjhu/imei")

        sc.stop()
    }
}
