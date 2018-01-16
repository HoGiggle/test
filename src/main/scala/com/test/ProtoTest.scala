package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/2/14.
  */
object ProtoTest {
    def main(args: Array[String]): Unit = {
        val Array(dayRange, partitions) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 注册要序列化的自定义类型。
        //        conf.registerKryoClasses(Array(classOf[InputUser]))
        val sc = new SparkContext(conf)


    }
}
