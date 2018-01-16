package com.tmp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/8.
  */
object SexRange {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val in = s"/user/iflyrd/work/jjhu/sex/appName/*/"
        //uid, pkg, appName, colDays, actDays, sex
        /*val tmp = sc.textFile(in).map(_.split("~")).map {
            case Array(uid, appName, date, colDays, actDays, sex) =>
                if (sex == "FEMALE") {
                    (uid, 0, 1)
                } else{
                    (uid, 1, 0)
                }
        }
            .distinct()
            .map(x => (x._2, x._3))
            .reduce{
                case (x, y) => (x._1 + y._1, x._2 + y._2)
            }
        println("Female = " + tmp._2 + ", Male = " + tmp._1)*/


        val tmp = sc.textFile(in).map(_.split("~"))
            .map {
                case Array(uid, appName, date, colDays, actDays, sex) =>
                    if (sex == "MALE") {
                        ((uid, appName, 0), date)
                    } else {
                        ((uid, appName, 1), date)
                    }
            }
            .reduceByKey {
                case (x, y) => if (x > y) x else y
            }
            .map { x => ((x._1._1, x._1._3), (x._1._2, 1)) }
            .reduceByKey {
                case (x, y) => (Seq(x._1, y._1).mkString(" "), x._2 + y._2)
            }
            .filter(x => x._2._2 <=3 || x._2._2 >= 80)
            .sortBy(_._2._2)
            .map{
                case ((uid, sex), (appNames, count)) => Seq(uid, sex, appNames, count).mkString("~")
            }
            .coalesce(1).saveAsTextFile("/user/iflyrd/work/jjhu/sex/excepApp")
        sc.stop()
    }
}
