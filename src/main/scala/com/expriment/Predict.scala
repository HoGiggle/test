package com.expriment

import com.util.CommonService
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/22.
  */
object Predict {
    def main(args: Array[String]): Unit = {
        val Array(useDate, dis, minAppNum, maxAppNum) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        //路径设置  模型加载
        val in = s"/user/iflyrd/work/jjhu/sex/appName/$useDate"
        val stopListPath = "/user/iflyrd/work/jjhu/sex/dataClean/exceptApp.txt"
        val stopList = sc.broadcast(sc.textFile(stopListPath).collect())
        val model = SVMModel.load(sc, s"/user/iflyrd/work/jjhu/sex/svm")

        /** *****************************************************************
          * app数量异常过滤
          */
        //uid, pkg, appName, colDays, actDays, sex
        val test = sc.textFile(in).map(_.split("~"))
            .map {
                case Array(uid, appName, date, colDays, actDays, sex) =>
                    if (sex == "MALE") {
                        ((uid, appName, -1), date)
                    } else {
                        ((uid, appName, 1), date)
                    }
            }
            .reduceByKey {
                //采集日期取最新日期
                case (x, y) => if (x > y) x else y
            }
            .map { x => ((x._1._1, x._1._3), x._1._2) }
            .groupByKey()
            .map {
                case ((uid, sex), apps) => {
                    val appArr = apps.toArray
                    (uid, sex, appArr.diff(stopList.value)) //无意义app过滤, 停用app
                }
            }
            .filter {
                //app安装数据过滤
                case (uid, sex, apps) => (apps.length > minAppNum.toInt) && (apps.length < maxAppNum.toInt)
            }
            .cache()

        /**
          * 1. 人为设置svm分割带距离, 分隔带以外的作为预测结果
          * 2. 云平台、预测结果一致
          * 3. 满足1、2条件认为性别可信
          * 4. 这批用户关联搜索词, 获取搜索词集
          */

        //训练集TF向量化
        val hashingTF = new HashingTF()
        val idf = CommonService.loadIdfHdfs(sc, "/user/iflyrd/work/jjhu/sex/model/idf")

        //测试集
        val testCol = test
            .map {
                case (uid, sex, seq) =>
                    val tf = hashingTF.transform(seq)
                    (uid, sex, idf.transform(tf), seq)
            }
            .map {
                case (uid, num, vector, feature) => {
                    val score = model.predict(vector)
                    (uid, num, score, feature)
                }
            }
            .filter{
                case (uid ,label, score, feature) => {
                    val isFar = Math.abs(score) >= dis.toDouble
                    val isFit = (label * score) >= 0
                    isFar && isFit
                }
            }
            .map{
                case (uid, label, score, feature) =>
                    Seq(uid, label, score, feature.mkString(":")).mkString("~")
            }
            .coalesce(32)
            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/SVMSample")

        sc.stop()
    }
}
