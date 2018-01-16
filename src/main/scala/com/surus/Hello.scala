package com.surus

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.surus.spark.RAD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jjhu on 2017/1/13.
  */
object Hello {
    def main(args: Array[String]): Unit = {
        onlineDataRun(Array("2", "10"))
//        demoDataRun(args)
    }

    def demoDataRun(args: Array[String]) = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
        val sc = new SparkContext(conf)

        //spark.RAD, not pig.RAD
        val rad = new RAD(7, 10)

        val in = "E:\\data\\surus_demo_data.txt"
        val data = sc.textFile(in).map(_.split("~"))
            .map{
                case Array(date, value) => (1, (date, value.toDouble))
            }
            .groupByKey()
            .map{
                case (index, input) => {
                    rad.exec(input.toArray)
                }
            }
            .flatMap(x => x)
        data.collect().foreach(println)

        sc.stop()
    }

    /**
      * 定制实现, 输入数据源格式固定, 格式不同解析部分调整
      * @param args
      */
    def onlineDataRun(args: Array[String]) = {
        //log level setting, and init sc
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val Array(timeCycle, mean) = args
        val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")//local debug, online need to delete
        val sc = new SparkContext(conf)

        //main run
        val data = sc.textFile("file:///E:\\data\\surus_online_data")  //输入路径, 自设, 或者配置为参数输入
            .map(inputDataParse)
            .filter{case (groupId, valueArr) => valueArr.length >= (3 * timeCycle.toInt)}//单一bugId, 输入量过滤, 根据需求自调
            .map{case (groupId, valueArr) => inputDataFormat(groupId, valueArr)}
            .filter {
                case (bugId, inputData, meanValue) => meanValue >= mean.toDouble //bug影响人数过滤, 人数太少, 突增没有意义
            }
            .map {
                case (bugId, inputData, meanValue) => {
                    //run rad
                    val radStandardData = getPreoorderTimeSeries(inputData, timeCycle.toInt)
                    val rad = new RAD(timeCycle.toInt, radStandardData.length / timeCycle.toInt)
                    (bugId, rad.exec(radStandardData))
                }
            }
            .map {
                case (bugId, dataArr) => {
                    val rushTimes = rushDefine(bugId, dataArr)
                    val isRush = if (rushTimes > 0) true else false
                    (bugId, isRush)
                }
            }

        //running result save or print
        data.coalesce(1).saveAsTextFile("file:///E:\\data\\surus")
        sc.stop()
    }

    /**
      * 自定义突增指标, 本实现当分解矩阵S value > 0.0 认为是突增行为
      * @param radResult Array("20170108~384938.0~-0.31489121862818303~5547.008364132352~-0.0~-17994.00836413235")
      * @return
      */
    def rushDefine(bugId: String, radResult: Array[String]): Int = {
        var rushTimes = 0
        println("bugId:" + bugId + ", len:" + radResult.length)
        radResult.map(_.split("~"))
            .filter(_.length == 6)  //numNonZeroRecords >= this.minRecords rad才会执行, 有些输入数据没有矩阵分解
            .foreach{
                case Array(date, originValue, tranValue, l, s, e) => {
                    if (s.toDouble > 0d){
                        rushTimes += 1
                    }
                }
            }
        rushTimes
    }

    /**
      * 输入数据规整
      * @param groupId
      * @param valueArr
      * @return
      */
    def inputDataFormat(groupId: String, valueArr: Array[String])
                        : (String, Array[(String, Double)], Double) = {
        //时间跨度
        val startDate = getDateKey(valueArr.min)
        val endDate = getDateKey(valueArr.max)

        //解析时间序列KV
        val itemMap = valueArr.map(x => {
            val Array(date, value) = x.split("~", -1)
            (date, value.toDouble)
        }).toMap

        //排序、填充时间序列, 作为RAD输入
        val dateRange = Utils.dateRange2StrList(startDate, endDate)
        val inputData = dateRange.map {
            date => (date, itemMap.getOrElse(date, 0d))
        }

        //group下value平均值, 过滤需要
        val meanValue = itemMap.values.sum / itemMap.size
        (groupId, inputData, meanValue)
    }

    /**
      * line like this: d7d64167ef8bd83bf866d84a76154c09,20170218~1#20170124~1
      * 输入数据解析, 根据输入数据结构修改
      * @param line
      * @return
      */
    def inputDataParse(line: String): (String, Array[String]) = {
        val Array(groupId, dateValueStr) = line.split(",", -1)
        val dateValueArr = dateValueStr.split("#", -1)
        (groupId, dateValueArr)
    }

    /**
      * 时间序列规整, 输入完整周期时间序列
      *
      * @param arr 填充完整的输入源数据, 间隔的日期填充为0
      * @param cycle 输入矩阵列数, 即周期宽度
      * @return
      */
    def getPreoorderTimeSeries(arr: Array[(String, Double)],
                               cycle: Int): Array[(String, Double)] = {
        require((arr != null) && (cycle > 0))

        val result: Array[(String, Double)] = new Array((arr.length / cycle) * cycle)
        for (i <- result.indices) {
            result(i) = arr(i)
        }
        result
    }

    /**
      * 20170203~1 => 20170203
      *
      * @param str
      * @return
      */
    def getDateKey(str: String): String = {
        str.split("~", -1)(0)
    }

    /**
      * ("20170301", "20170303") => Array[String]("20170301", "20170302", "20170303")
      * @param startDateStr
      * @param endDateStr
      * @return
      */
    def dateRange2StrList(startDateStr: String,
                          endDateStr: String): Array[String] = {
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val calendar = Calendar.getInstance()
        calendar.setTime(sdf.parse(startDateStr))

        val result: ArrayBuffer[String] = ArrayBuffer()
        val endDate = sdf.parse(endDateStr)
        while (calendar.getTime.before(endDate)){
            result += sdf.format(calendar.getTime)
            calendar.add(Calendar.DAY_OF_YEAR, 1)
        }
        result += endDateStr

        result.toArray
    }
}
