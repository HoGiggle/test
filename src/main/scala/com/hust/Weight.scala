package com.hust

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.SVMModel

/**
  * Created by jjhu on 2016/12/14.
  */
object Weight {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
        val sc = new SparkContext(conf)

        val model = SVMModel.load(sc, "file:///E:\\model\\svmAll")
        val mapPath = "file:///E:\\data\\wordsMapping.txt"
        val wordsMapping = sc.textFile(mapPath)

    }
}
