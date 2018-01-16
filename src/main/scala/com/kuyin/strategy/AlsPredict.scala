package com.kuyin.strategy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/3/9.
  */
object AlsPredict {
    def main(args: Array[String]): Unit = {
        val Array(date) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        //path
        val modelPath = s"/user/iflyrd/kuyin/strategy/ALS/model/$date"
        val model = MatrixFactorizationModel.load(sc, modelPath)

        val userFt = model.userFeatures.cache()
        val productFt = model.productFeatures

        println("user length = " + userFt.count())
        println("product length = " + productFt.count())


    }
}
