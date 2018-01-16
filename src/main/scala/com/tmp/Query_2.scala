package com.tmp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/22.
  */
object Query_2 {
    def main(args: Array[String]): Unit = {
        val Array(useDate) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        // path init
        val queryPath = s"/user/iflyrd/work/jjhu/sex/query/segment/$useDate"
        val sexMarkPath = "/user/iflyrd/work/jjhu/sex/SVMSample"
        val separator = sc.broadcast(":") //"~"

        //data init & main logic
        val sexMarkData = sc.textFile(sexMarkPath).map(_.split("~"))
            .filter(_.length >= 4)
            .map{
                case Array(uid, label, score, apps) => (uid, label)
            }

        val qData = sc.textFile(queryPath).map(_.split(","))
                .filter(x => x.length >= 3 && x(0).length >= 15 && x(1).length == 15 && x(2).length > 0)
                .map{
                    case Array(uid, imei, seg) => {
                        ((uid, imei), seg)
                    }
                }
//            .filter(x => x.length >= 6 && x(1).length >= 15
//                && x(0).length == 18 && x(5).length > 0)
//            .map{
//                case Array(uid, imei, query, pkg, date, seg) => {
//                    val segs = seg.split(",")
//                    val words = segs.map{
//                        item => {
//                            val Array(word, des) = item.split(":")
//                            word
//                        }
//                    }
//                    ((uid, imei), words.mkString("~"))
//                }
//            }
            .reduceByKey{
                case (x, y) => Seq(x, y).mkString(separator.value)
            }
            .map{
                case ((uid, imei), seg) => (uid, (1, seg))
            }
            .reduceByKey{
                case (x, y) => (x._1 + y._1, Seq(x._2, y._2).mkString(separator.value))
            }
            .filter(_._2._1 == 1)
            .map{
                case (uid, (count, seg)) => (uid, seg)
            }

        val wordCol = sexMarkData.join(qData).flatMap{
            case (uid, (label, words)) => words.split(separator.value).map{
                word => {
                    if (label == "-1") { //male
                        (word, (0, 1))
                    } else {             //female
                        (word, (1, 0))
                    }
                }
            }
        }
            .reduceByKey{
                case (x, y) => (x._1 + y._1, x._2 + y._2)
            }
            .cache()

        wordCol.filter(x => x._2._1 > 0)
            .sortBy(_._2._1, false)
            .map{
                case (word, (female, male)) => Seq(word, female).mkString("~")
            }
            .coalesce(32)
            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/query/femaleWords_1")

        wordCol.filter(x => x._2._2 > 0)
            .sortBy(_._2._2, false)
            .map{
                case (word, (female, male)) => Seq(word, male).mkString("~")
            }
            .coalesce(32)
            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/query/maleWords_1")

        //close doing
        wordCol.unpersist()
        sc.stop()
    }
}
