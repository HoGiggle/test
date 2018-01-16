package com.expriment

import com.util.CommonService
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/15.
  */
object AnyAnalysis {
    def main(args: Array[String]): Unit = {
        val Array(date) = args
        appRange_1(date)
    }

    def badCase() = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val model = SVMModel.load(sc, "/user/iflyrd/work/jjhu/sex/svm")// file:///E:\\inputData\\svm
        val mapping = sc.broadcast(sc.textFile("/user/iflyrd/work/jjhu/sex/dataClean/mapping").map(_.split("~"))  // file:///E:\inputData\mapping.txt
            .map{
            case Array(index, appName) => (index.toInt, appName)
        }
            .collect().toMap
        )
        val test = sc.objectFile[LabeledPoint]("/user/iflyrd/work/jjhu/sex/testFill_10_3_80/") // file:///E:\inputData\test
        // Compute raw scores on the test set.
        val scoreAndLabels = test.map { point =>
            val score = model.predict(point.features)
            if (point.label == 0d){
                (score * (-1), point.label, point.features.toSparse)
            }else {
                (score, point.label, point.features.toSparse)
            }
        }
            .filter(_._1 < 0)
            .map{
                case (dis, label, feature) => {
                    val apps = feature.indices.map(index => mapping.value(index))
                    (dis, label, apps.mkString(":"))
                }
            }
            .sortBy(_._1)
            .coalesce(1)
            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/badcase")  //file:///E:\inputData\badcase

        sc.stop()
    }

    def appRange(dateRange:String): Unit ={
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val in = s"/user/iflyrd/work/jjhu/sex/appName/$dateRange"
        val tmp = sc.textFile(in).map(_.split("~"))
            .filter{
                case Array(uid, app, date, colDate, useDate, sex) => {
                    val use = useDate.toInt
                    val col = colDate.toInt
                    (use > 0) && (col >= use)
                }
            }
            .map{
                case Array(uid, app, date, colDate, useDate, sex) => {
                    ((uid, app), (date, colDate.toInt, useDate.toInt))
                }
            }
            .reduceByKey{
                case (x, y) => if (x._1 > y._1) x else y
            }
            .map{
                case ((uid, app), (date, colDate, useDate)) => (uid, (useDate * 1.0d / colDate, 1))
            }
            .reduceByKey{
                case (x, y) => (x._1 + y._1, x._2 + y._2)
            }
            .map{
                case (uid, (freq, num)) => (CommonService.formatDouble(freq/num, 2), 1)
            }
            .reduceByKey(_ + _)
            .sortBy(_._1, false)
            .map(x => Seq(x._1, x._2).mkString("~"))
            .coalesce(1).saveAsTextFile("/user/iflyrd/work/jjhu/sex/useful")

        sc.stop()
    }

    def appRange_1(dateRange:String): Unit ={
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val in = s"/user/iflyrd/work/jjhu/sex/appName/$dateRange"
        val tmp = sc.textFile(in).map(_.split("~"))
            .filter{
                case Array(uid, app, date, colDate, useDate, sex) => {
                    val use = useDate.toInt
                    val col = colDate.toInt
                    (use > 0) && (col >= use)
                }
            }
            .map{
                case Array(uid, app, date, colDate, useDate, sex) => {
                    ((uid, app), (date, colDate.toInt, useDate.toInt))
                }
            }
            .reduceByKey{
                case (x, y) => if (x._1 > y._1) x else y
            }
            .map{
                case ((uid, app), (date, colDate, useDate)) => (uid, (colDate, useDate))
            }
            .reduceByKey{
                case (x, y) => (x._1 + y._1, x._2 + y._2)
            }
            .map{
                case (uid, (col, use)) => (CommonService.formatDiv(use, col, 2), 1)
            }
            .reduceByKey(_ + _)
            .sortBy(_._1, false)
            .map(x => Seq(x._1, x._2).mkString("~"))
            .coalesce(1).saveAsTextFile("/user/iflyrd/work/jjhu/sex/useful_1")

        sc.stop()
    }


    def analysisScore(sc: SparkContext,
                      in: String = "file:///E:\\inputData\\score.txt",
                      out: String = "file:///E:\\inputData\\scoreRes") = {
        sc.textFile(in).map(_.split(","))
            .map{
                case Array(score, label) => (score.substring(1, score.length), label.substring(0, label.length-1))
            }
            .map{
                case (score, label) => ((Math.round(score.toDouble), label.toInt), 1)
            }
            .reduceByKey(_ + _).cache()
            .map{
                case ((score, label), count) => Seq(label, score, count).mkString("~")
            }
            .coalesce(1).saveAsTextFile(out)
    }
}
