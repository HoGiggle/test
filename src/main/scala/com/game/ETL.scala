package com.game

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jjhu on 2017/3/25.
  */
case class TwoOrder(first: Int, second: Int) extends Ordered[TwoOrder] with Serializable {
  override def compare(that: TwoOrder): Int = {
    if (this.first != that.first) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}

object ETL {
  def main(args: Array[String]): Unit = {
    actionETL(args)
  }

  def allInfoRange(args: Array[String]) = {
    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val path = "file:///E:\\data\\ccf_jd\\product_raw_data"
    val rawLog = sc.textFile(path).map(_.split("~", -1))
      .filter(_.length == 6)
      .map{
        case Array(itemId, attr1, attr2, attr3, cate, brand) =>
          (itemId, attr1, attr2, attr3, cate, brand)
      }.cache()
    println("Product RDD size = " + rawLog.count())
    println("Product itemId size = " + rawLog.map(_._1).distinct().count())

    /*//brand
    rawLog.map{
      case (itemId, attr1, attr2, attr3, cate, brand) => (brand, 1)
    }
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .map{
        case (brand, cnt) => Seq(brand, cnt).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\brand")

    //attr1
    rawLog.map{
      case (itemId, attr1, attr2, attr3, cate, brand) => (attr1, 1)
    }
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .map{
        case (brand, cnt) => Seq(brand, cnt).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\attr1")

    //attr2
    rawLog.map{
      case (itemId, attr1, attr2, attr3, cate, brand) => (attr2, 1)
    }
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .map{
        case (brand, cnt) => Seq(brand, cnt).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\attr2")

    //attr3
    rawLog.map{
      case (itemId, attr1, attr2, attr3, cate, brand) => (attr3, 1)
    }
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .map{
        case (brand, cnt) => Seq(brand, cnt).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\attr3")

    //combine attr
    rawLog.map{
      case (itemId, attr1, attr2, attr3, cate, brand) => ((attr1, attr2, attr3), 1)
    }
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .map{
        case ((a1, a2, a3), cnt) => Seq(a1, a2, a3, cnt).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\combine_attr")*/

    //attr3
    val complete = rawLog.filter{
      case (itemId, attr1, attr2, attr3, cate, brand) => {
        (attr1.toInt > 0) && (attr2.toInt > 0) && (attr3.toInt > 0)
      }
    }.count()
    println("Attr complete product = " + complete)



    rawLog.unpersist()
    sc.stop()
  }

  def userAnalysis(args: Array[String]) = {
    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val path = "file:///E:\\data\\ccf_jd\\user_raw_data"
    val rawLog = sc.textFile(path).map(_.split("~"))
      .map{
        case Array(uid, age, sex, level, regDate) => (uid.toInt, age.toInt, sex.toInt, level.toInt, regDate.toInt)
      }.cache()

    /*println("RDD size = " + rawLog.count())
    //uid distinct
    println("Uid size = " + rawLog.map(_._1).distinct().count())

    //sex
    rawLog.map{
      case (uid, age, sex, level, regDate) => (sex, 1)
    }
      .reduceByKey(_ + _)
      .collect()
      .foreach{
        case (sex, count) => println(s"Sex $sex = $count")
      }

    //age
    /**
      * -1  未知
      *  0  15及以下
      *  1  16-25
      *  2  26-35
      *  3  36-45
      *  4  46-55
      *  5  56及以上
      */
    rawLog.map{
      case (uid, age, sex, level, regDate) => ((age, sex), 1)
    }
      .reduceByKey(_ + _)
      .map{
        case ((age, sex), count) => (TwoOrder(age, sex), count)
      }
      .sortByKey()
      .map{
        case (two, count) => Seq(two.first, two.second, count).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\age_sex")

    //level
    rawLog.map{
      case (uid, age, sex, level, regDate) => ((level, age), 1)
    }
      .reduceByKey(_ + _)
      .map{
        case ((level, age), count) => (TwoOrder(level, age), count)
      }
      .sortByKey()
      .map{
        case (two, count) => Seq(two.first, two.second, count).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\level_age")

    rawLog.map{
      case (uid, age, sex, level, regDate) => ((level, sex), 1)
    }
      .reduceByKey(_ + _)
      .map{
        case ((level, sex), count) => (TwoOrder(level, sex), count)
      }
      .sortByKey()
      .map{
        case (two, count) => Seq(two.first, two.second, count).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\level_sex")

    //regDate
    val df = new SimpleDateFormat("yyyyMMdd")
    val now = df.parse("20160415").getTime
    rawLog.map{
      case (uid, age, sex, level, regDate) => {
        val regDay = df.parse(regDate.toString)
        val dis = (now - regDay.getTime) / (1000l * 3600 * 24)
        val regLevel = dis / 180

        //5年及以上全为10
        if (regLevel >= 10) {
          ((10, level), 1)
        } else {
          ((regLevel.toInt, level), 1)
        }
      }
    }
      .reduceByKey(_ + _)
      .map{
        case ((reg, level), count) => (TwoOrder(reg, level), count)
      }
      .sortByKey()
      .map{
        case (two, count) => Seq(two.first, two.second, count).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\reg_level")*/


    rawLog.unpersist()
    sc.stop()
  }

  def actionETL(args: Array[String]) = {
    val Array(startDate, endDate) = args

    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val formatDF = new SimpleDateFormat("yyyyMMddHHmmss")
    val parseDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateRangeArr = dateCreate(startDate, endDate)

    // HDFS path setting & broadcast
    val path = "/user/iflyrd/work/zqwu/jd/data/JData_Action_*"
    val rawRDD = sc.textFile(path, 8).map(_.split(",", -1))
      .filter(x => (x.length == 7) && (x(2).length == 19))
      .map{
        case Array(uid, itemId, time, modelId, behaviorType, cate, brand) => {
          var formatTime = "19990101"
          try{
            formatTime = formatDF.format(parseDF.parse(time))
          } catch {
            case e: Exception => println("Action exception as : " + time)
          }

          (formatTime.substring(0, 8), Seq(uid.toDouble.toInt, itemId.toInt, time, modelId, behaviorType.toInt, cate.toInt, brand.toInt).mkString("~"))
        }
      }
      .cache()
    println(rawRDD.count())

    for (oneDay <- dateRangeArr) {
      val bOneDay = sc.broadcast(oneDay)
      rawRDD.filter{
        case (date, value) => {
          date == bOneDay.value
        }
      }.map{
        case (date, value) => value
      }.saveAsTextFile(s"/user/iflyrd/work/jjhu/ccf_jd_v1/action/${bOneDay.value}")
      bOneDay.unpersist()
    }

    rawRDD.unpersist()
    sc.stop()
  }

  def dateCreate(start: String, end: String): Array[String] = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val sDate = sdf.parse(start)
    val eDate = sdf.parse(end)

    val cal = Calendar.getInstance()
    cal.setTime(sDate)
    val endCal = Calendar.getInstance()
    endCal.setTime(eDate)
    val arr = new ArrayBuffer[String]()
    while(cal.before(endCal) || cal.equals(endCal)) {
      arr.append(sdf.format(cal.getTime))
      cal.add(Calendar.DAY_OF_YEAR, 1)
    }

    arr.toArray
  }

  def commentETL(args: Array[String]) = {
    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // HDFS path setting & broadcast
    val path = "file:///E:\\data\\ccf_jd\\comment"
    val proPath = "file:///E:\\data\\ccf_jd\\product"
    val parseDF = new SimpleDateFormat("yyyy-MM-dd")
    val formatDF = new SimpleDateFormat("yyyyMMdd")

    val rawLog = sc.textFile(path).map(_.split(","))
      .filter(_.length == 5)
      .map{
        case Array(date, itemId, commNum, badComm, badCommRate) => {
          var formatDate = ""
          try {
            formatDate = formatDF.format(parseDF.parse(date))
          } catch {
            case e: Exception => println("Date exception as : " + date)
          }
          (formatDate, itemId, commNum, badComm, badCommRate)
        }
      }.cache()
    println("comment raw size = " + rawLog.count())

    //raw log save
    rawLog.map{
      case (formatDate, itemId, commNum, badComm, badCommRate) =>
        Seq(formatDate, itemId, commNum, badComm, badCommRate).mkString("~")
    }.coalesce(1).saveAsTextFile("file:///E:\\data\\ccf_jd\\comm_raw")

    //valid comment
    val productRDD = sc.textFile(proPath).map(_.split("~"))
      .map{
        case Array(itemId, attr1, attr2, attr3, cate, brand) => itemId
      }
    val bProduct = sc.broadcast(productRDD.collect().toSet)
    rawLog.filter{
      case (formatDate, itemId, commNum, badComm, badCommRate) => bProduct.value.contains(itemId)
    }
      .map{
        case (formatDate, itemId, commNum, badComm, badCommRate) =>
          Seq(formatDate, itemId, commNum, badComm, badCommRate).mkString("~")
      }
      .coalesce(1).saveAsTextFile("file:///E:\\data\\ccf_jd\\comm_valid")


    rawLog.unpersist()
    sc.stop()
  }

  def userETL(args: Array[String]) = {
    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val parseDf = new SimpleDateFormat("yyyy-MM-dd")
    val formatDf = new SimpleDateFormat("yyyyMMdd")

    // HDFS path setting & broadcast
    val path = "file:///E:\\data\\ccf_jd\\user"
    sc.textFile(path).map(_.split(","))
      .filter(_.length == 5)
      .filter(x => !((x(1).toUpperCase() == "NULL") ||
        (x(2).toUpperCase() == "NULL") || (x(4).toUpperCase() == "NULL")))
      .map{
        case Array(uid, age, sex, level, regDate) => {
          /**
            * -1  未知
            *  0  15及以下
            *  1  16-25
            *  2  26-35
            *  3  36-45
            *  4  46-55
            *  5  56及以上
            */
          var ageLevel = -1
          if (age.length < 2){
            println("Age exception as : " + age)
          }
          if (age.length == 2){
            ageLevel = -1
          } else {
            val ageHead = age.substring(0, 2)
            ageHead match {
              case "15" => ageLevel = 0
              case "16" => ageLevel = 1
              case "26" => ageLevel = 2
              case "36" => ageLevel = 3
              case "46" => ageLevel = 4
              case "56" => ageLevel = 5
              case _ => ageLevel = -2
            }
          }

          var formatDate = ""
          try{
            formatDate = formatDf.format(parseDf.parse(regDate))
          } catch {
            case e: Exception => println("RegDate exception as : " + regDate)
          }

          Seq(uid, ageLevel, sex, level, formatDate).mkString("~")
        }
      }
      .coalesce(1)
      .saveAsTextFile("file:///E:\\data\\ccf_jd\\jd_user")

    sc.stop()
  }
}
