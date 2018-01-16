package com.util

import java.io._
import java.text.NumberFormat

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDFModel}
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
  * Created by jjhu on 2016/12/15.
  */
object CommonService {

  /**
    * 搜狗词库格式更新为HanLp所需
    *
    * @param in
    * @param out
    * @return
    */
  def sougouDataLoad(in: String,
                     out: String) = {
    val data = Source.fromFile(new File(in), "utf-8").getLines().toArray
    val bw = new BufferedWriter(new FileWriter(new File(out)))
    data.foreach {
      line => {
        val Array(fre, word) = line.split(" ", -1)
        bw.write(word)
        bw.write("\n")
      }
    }

    bw.flush()
    bw.close()
  }

  /**
    * tf-idf index appName映射, 存储
    *
    * @param data
    * @param path
    */
  def saveFeatureMapping(data: RDD[(Int, Array[String])],
                         path: String) = {
    val hashingTF = new HashingTF()
    data.flatMap(_._2)
      .distinct()
      .map {
        appName => Seq(hashingTF.indexOf(appName), appName).mkString("~")
      }.coalesce(1).saveAsTextFile(path)
  }

  /**
    * idf model save
    *
    * @param sc
    * @param out
    * @param idf
    */
  def saveIdfModel(sc: SparkContext,
                   out: String,
                   idf: IDFModel) = {
    val hadoopConf = sc.hadoopConfiguration
    val fileSystem = FileSystem.get(hadoopConf)
    val path = new Path(out)
    val oos = new ObjectOutputStream(new FSDataOutputStream(fileSystem.create(path)))
    oos.writeObject(idf)
    oos.close
  }

  /**
    * 加载本地IDF model
    *
    * @param in
    * @return
    */
  def loadIdfLocal(in: String): IDFModel = {
    val ois = new ObjectInputStream(new FileInputStream(in))
    ois.readObject.asInstanceOf[IDFModel]
  }


  /**
    * 加载hdfs上IDF model
    *
    * @param sc
    * @param in
    * @return
    */
  def loadIdfHdfs(sc: SparkContext,
                  in: String): IDFModel = {
    val hadoopConf = sc.hadoopConfiguration
    val fileSystem = FileSystem.get(hadoopConf)
    val path = new Path(in)
    val ois = new ObjectInputStream(fileSystem.open(path))
    ois.readObject().asInstanceOf[IDFModel]
  }

  /**
    * 保留decimalBit位除法
    *
    * @param div1
    * @param div2
    * @param decimalBit
    * @return
    */
  def formatDiv(div1: Int,
                div2: Int,
                decimalBit: Int): String = {
    if (div2 == 0) return "0"
    val nFormat = NumberFormat.getNumberInstance
    nFormat.setMaximumFractionDigits(decimalBit)
    nFormat.format(div1 * 1.0d / div2)
  }


  /**
    * Double 保留小数位
    *
    * @param in
    * @param decimalBit
    * @return
    */
  def formatDouble(in: Double,
                   decimalBit: Int): String = {
    val nFormat = NumberFormat.getNumberInstance
    nFormat.setMaximumFractionDigits(decimalBit)
    nFormat.format(in)
  }

  /**
    * 加载hdfs lzo文件
    *
    * @param sc
    * @param path
    * @return
    */
  def readLzoFile(sc: SparkContext, path: String): RDD[String] = {
    sc.newAPIHadoopFile(path,
      classOf[com.hadoop.mapreduce.LzoTextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable],
      classOf[org.apache.hadoop.io.Text])
      .map(_._2.toString)
  }

  /**
    * * @input       String
    * * @output      Map
    * * @func        String -> Map
    *
    */
  def MapLoader(line: String): Map[String, String] = {
    //the separator between Maps
    val sepTerm = 31.toChar.toString
    //the separator between KVs
    val sepKv = "~"
    val terms = line.split(sepTerm)
    val result = scala.collection.mutable.Map[String, String]()
    for (term <- terms) {
      val kvs = term.split(sepKv, 2)
      //if key is null or value is null, then don't insert into the Map
      if (kvs.length > 1 && kvs(0).nonEmpty && kvs(1).nonEmpty) {
        //key toLowerCase
        result += (kvs(0).toLowerCase() -> kvs(1))
      }
    }
    result.toMap
  }

  /**
    * 判断输入字符串是否为IP地址
    *
    * @param ip
    * @return
    */
  def isIP(ip: String): Boolean = {
    if (ip == null || ip.length == 0)
      return false

    val p = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))"
    ip.matches(p)
  }

  /**
    * IP转为long型整数
    *
    * @param ip
    * @return
    */
  def ip2Long(ip: String): Long = {
    var ipLong: Long = 0
    if (isIP(ip)) {
      val ips = ip.split("\\.")
      ipLong = ips.mkString("").toLong
    } else ipLong = 0l

    ipLong
  }

  /**
    * 判断IP是否为公网地址
    *
    * @param ip
    * @return
    */
  def isPublicIP(ip: String): Boolean = {
    if (isIP(ip)) {
      val aTypeMin = 10000l
      val aTypeMax = 10255255255l
      val bTypeMin = 1721600l
      val bTypeMax = 17231255255l
      val cTypeMin = 19216800l
      val cTypeMax = 192168255255l

      val ipLong = ip2Long(ip)
      var isPublic = true
      if ((ipLong >= aTypeMin) && (ipLong <= aTypeMax)) {
        isPublic = false
      } else if ((ipLong >= bTypeMin) && (ipLong <= bTypeMax)) {
        isPublic = false
      } else if ((ipLong >= cTypeMin) && (ipLong <= cTypeMax)) {
        isPublic = false
      } else {}

      isPublic
    } else false
  }

  /**
    * acdata解析联系人信息
    *
    * @param line acdata一行
    * @return normalNum~exceptNum
    */
  def getPhoneNums(line: String): (Set[String], Set[String]) = {
    var normalNum: Set[String] = Set()
    var exceptNum: Set[String] = Set()
    import org.json4s.native.JsonMethods._
    val json = parse(line)
    val jsonMap = json.values.asInstanceOf[Map[String, _]]

    if (jsonMap.contains("contactInfo")) {
      for (item <- jsonMap.getOrElse("contactInfo", "").asInstanceOf[List[_]]) {
        for ((name, num) <- item.asInstanceOf[Map[String, Map[String, List[_]]]]) {
          for (sis <- num.getOrElse("sis", "").asInstanceOf[List[Map[String, _]]]) {
            try {
              for (nm <- sis.getOrElse("nm", "").asInstanceOf[List[Map[String, _]]]) {
                val tmp = nm.get("ifi")
                tmp match {
                  case Some(str: String) => {
                    val phone = cleanPhoneNum(str) //号码解析、处理模块 todo 需要维护
                    if (isPhoneNum(phone)) normalNum = normalNum.+(phone)
                    else exceptNum = exceptNum.+(phone)
                  }
                }
              }
            } catch {
              case e: Exception => {
                //                                println(uid + " " + sis)
              }
            }
          }
        }
      }
    }
    (normalNum, exceptNum)
  }

  /**
    * acdata解析联系人信息
    *
    * @param line acdata一行
    * @return Set(uid~name~phoneNum)
    */
  def parsePhoneNum(line: String): Set[String] = {
    val map = CommonService.MapLoader(line)
    val uid = map.getOrElse("uid", "")
    val contactInfo = map.getOrElse("flumedesds", "")

    var normalNum: Set[String] = Set()
    var exceptNum: Set[String] = Set()
    import org.json4s.native.JsonMethods._
    val json = parse(contactInfo)
    val jsonMap = json.values.asInstanceOf[Map[String, _]]

    if (jsonMap.contains("contactInfo") && uid.length > 0) {
      for (item <- jsonMap.getOrElse("contactInfo", "").asInstanceOf[List[_]]) {
        for ((name, num) <- item.asInstanceOf[Map[String, Map[String, List[_]]]]) {
          val cleanName = name.trim.replaceAll("\\~", "")
          for (sis <- num.getOrElse("sis", "").asInstanceOf[List[Map[String, _]]]) {
            try {
              for (nm <- sis.getOrElse("nm", "").asInstanceOf[List[Map[String, _]]]) {
                val tmp = nm.get("ifi")
                tmp match {
                  case Some(str: String) => {
                    val phone = cleanPhoneNum(str) //号码解析、处理模块 todo 需要维护
                    if (isPhoneNum(phone)) normalNum = normalNum.+(Seq(uid, cleanName, phone).mkString("~"))
                    else exceptNum = exceptNum.+(Seq(uid, cleanName, phone).mkString("~"))
                    //println(cleanName + " " + phone)
                  }
                }
              }
            } catch {
              case e: Exception => {
                //                                println(uid + " " + sis)
              }
            }
          }
        }
      }
    }
    normalNum
  }

  /**
    * 输入解析到的原始电话号码, 如果全部为数字则认为是正常号码
    *
    * @param call
    * @return
    */
  def isPhoneNum(call: String): Boolean = {
    val reg = "\\d" //电话号码因为包含服务号等信息, 长度、正则不方便用, 全为数字即可, 使用时根据需求转化
    (call.length == 11 || call.length == 13) && call.replaceAll(reg, "").length == 0 //11位号码或者+86
  }

  /**
    * 输入解析到的原始电话号码, 输出清洗后子串
    *
    * @param call
    * @return
    */
  def cleanPhoneNum(call: String): String = {
    if (call == null || call.length == 0) return call
    call.trim.replaceAll("[\\s+\\-\\+~,' ']", "") //' '目测是中文的空格
  }

  def scLocalInit(jobName: String): SparkContext = {
    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(jobName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc
  }

  def scClusterInit(jobName: String): SparkContext = {
    // Spark log level & sc init
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)
    sc
  }
}
