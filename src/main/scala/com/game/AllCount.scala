package com.game

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/4/7.
  */
object AllCount {
  def main(args: Array[String]): Unit = {

    val Array(dateRange) = args
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //user
    val user = sc.textFile("/user/iflyrd/work/jjhu/ccf_jd_v1/user/user_raw")
      .map(_.split("~", -1))
      .map(x => x(0))
      .distinct()
    val bUser = sc.broadcast(user.collect().toSet)
    println("User size = " + bUser.value.size)

    //product
    val product = sc.textFile("/user/iflyrd/work/jjhu/ccf_jd_v1/product/product_raw")
      .map(_.split("~", -1))
      .map(x => x(0))
      .distinct()
    val bProduct = sc.broadcast(product.collect().toSet)
    println("Product size = " + bProduct.value.size)


    //comment
    val comment = sc.textFile("/user/iflyrd/work/jjhu/ccf_jd_v1/comment/comment_raw")
      .map(_.split("~", -1))
      .map(x => x(1))
      .distinct()
      .filter(bProduct.value.contains(_))
    println("Product has comment = " + comment.count())

    //action
    val actPath = s"/user/iflyrd/work/jjhu/ccf_jd_v1/action/$dateRange"
    val action = sc.textFile(actPath)
      .map(_.split("~", -1))
      .cache()

    val pInAct = action.map(x => x(1)).distinct().filter(bProduct.value.contains(_))
    val uInAct = action.map(x => x(0)).distinct().filter(bUser.value.contains(_))
    println("Product in action = " + pInAct.count())
    println("User in action = " + uInAct.count())

    sc.stop()
  }
}
