package com.util

import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.JsonAST.JValue

/**
  * Created by jjhu on 2017/2/28.
  */
object ETLUtil {

    /**
      * type_80000目录日志读取
      * @param sc
      * @param path
      * @return
      */
    def readMap(sc: SparkContext,
                path: String): RDD[Map[String, String]] = {
        sc.objectFile[Map[String, String]](path)
    }

    /**
      * Json4s解析json字符串, 返回JValue
      * @param jsonStr
      * @return
      */
    def json4sParse(jsonStr: String): JValue = {
        import org.json4s.native.JsonMethods._
        val jValue = parse(jsonStr)
        jValue
    }

    /**
      * flume日志解析为String
      * @param sc
      * @param filepath
      * @return
      */
    def loadSequence2Str(sc: SparkContext, filepath: String): RDD[String] = {
        val input = sc.sequenceFile(filepath, classOf[LongWritable], classOf[BytesWritable])
            .map(x => new String(x._2.getBytes, 0, x._2.getLength))
        input
    }
}
