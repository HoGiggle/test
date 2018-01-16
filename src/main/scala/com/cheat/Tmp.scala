package com.cheat

import com.util.CommonService
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/2/21.
  */
object Tmp {
    def main(args: Array[String]): Unit = {
        val Array(fdf, dayRange) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val contactPath = s"/user/iflyms/flume/$dayRange/*/*acdata*{0,1,2,3,4,5,6,7,8,9}/"

        val contact = sc.textFile(contactPath)
            .map(CommonService.MapLoader)
            .filter(m => (m.getOrElse("df", "") == fdf) && m.contains("flumedesds"))
            .map(m => m.getOrElse("flumedesds", ""))
            .coalesce(1)
            .saveAsTextFile(s"/user/iflyrd/work/jjhu/simulator/acdata/$fdf/")
        sc.stop()
    }
}
