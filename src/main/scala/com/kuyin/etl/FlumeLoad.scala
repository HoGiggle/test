package com.kuyin.etl

import com.kuyin.structure.KYLSLogField
import com.util.ETLUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Type = 80000 酷音用户行为日志ETL
  *1. 剔除字段：
  * reqname
  * ret
  * retdesc
  * resobjPe
  * dur
  *
  *2. Ext解析字段
  * ringname
  * singername
  *
  * Created by jjhu on 2017/2/27.
  */
object FlumeLoad {
  def main(args: Array[String]): Unit = {
    // Programmer arguments init:
    val Array(logDate, partitionNum, logType) = args

    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    // HDFS path setting & broadcast
    val path = s"/user/iflyrd/kuyin/input/rawlog/$logDate/*/*/stable/data*/"
    val exceptKey = sc.broadcast(
      Set(KYLSLogField.SYS_Dur,
        KYLSLogField.SYS_RetDesc,
        KYLSLogField.SYS_ReqName,
        KYLSLogField.SYS_Ret,
        KYLSLogField.SYS_ResObj))

    // main coding
    val rawRDD = ETLUtil.loadSequence2Str(sc, path)
    val mapRDD = rawRDD.filter(str => str.contains(logType)) //日志type字段过滤
      .map(line => etlStr2Map(line, exceptKey.value))
      .filter(m => m.nonEmpty && logTypeFilter(m, logType)) //日志type字段过滤, Double
    mapRDD.coalesce(partitionNum.toInt).saveAsObjectFile(s"/user/iflyrd/kuyin/input/type_$logType/$logDate") //Map[String, String]
    sc.stop()
  }


  /**
    * 日志type字段过滤
    *
    * @param map   etlStr2Map输出
    * @param value type字段值
    * @return
    */
  def logTypeFilter(map: Map[String, String],
                    value: String): Boolean = {
    map.contains(KYLSLogField.TYPE) && (map(KYLSLogField.TYPE) == value)
  }


  /**
    * 解析type = 80000日志, string => Map[String, String]
    *
    * @param line
    * @return
    */
  def etlStr2Map(line: String,
                 exceptKey: Set[String]): Map[String, String] = {
    val result = collection.mutable.Map[String, String]()

    try {
      import org.json4s.native.JsonMethods._
      val jsonMap = parse(line).values.asInstanceOf[Map[String, _]]

      for ((key, value) <- jsonMap) {
        val lowKey = key.toLowerCase
        if (!exceptKey.contains(lowKey)) {//剔除不需要字段
          if (KYLSLogField.EXT == lowKey) { //todo 存在ext为空异常情况，目前逻辑不变
            //解析ext字段
            parseExtField(value, result)
          } else {
            //其他字段均为String, 直接添加
            parseNormalField(lowKey, value, result)
          }
        }
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

    result.toMap
  }

  /**
    * 解析正常key->value格式数据, value为基本数据类型
    *
    * @param key
    * @param value
    * @param result
    * @return
    */
  def parseNormalField(key: String,
                       value: Any,
                       result: collection.mutable.Map[String, String]): collection.mutable.Map[String, String] = {
    value match {
      case x: String => result += (key -> x)
      case _ => result += (key -> value.toString)
    }
    result
  }

  /**
    * 解析ext字段, 添加ringname & singername
    *
    * @param value
    * @param result
    * @return
    */
  def parseExtField(value: Any,
                    result: collection.mutable.Map[String, String]): collection.mutable.Map[String, String] = {
    value match {
      case extValue: List[Any] => {
        if (extValue.nonEmpty) {
          extValue.head match {
            case extMapValue: Map[String, String] => {
              if (extMapValue.contains(KYLSLogField.EXT_RingName)) { //歌名
                result += (KYLSLogField.EXT_RingName ->
                  extMapValue.getOrElse(KYLSLogField.EXT_RingName, KYLSLogField.STRING_VALUE_DEFAULT))
              }

              if (extMapValue.contains(KYLSLogField.EXT_SingerName)) { //歌手名
                result += (KYLSLogField.EXT_SingerName ->
                  extMapValue.getOrElse(KYLSLogField.EXT_SingerName, KYLSLogField.STRING_VALUE_DEFAULT))
              }

              if (extMapValue.contains(KYLSLogField.EXT_LisDur)) { //试听时间 /ms
                result += (KYLSLogField.EXT_LisDur ->
                  extMapValue.getOrElse(KYLSLogField.EXT_LisDur, KYLSLogField.INT_VALUE_DEFAULT))
              }
            }
          }
        }
      }
      case _ => throw new Exception()  //异常分支
    }
    result
  }
}
