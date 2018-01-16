package com.expriment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/21.
  */
object SVMTest_3 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
        val sc = new SparkContext(conf)

        val model = SVMModel.load(sc, "file:///E:\\inputData\\svm")
        val hashingTF = new HashingTF()
        val weight = model.weights.toDense.values

        val tmp = sc.textFile("file:///E:\\inputData\\bad.txt").map(_.split("~"))
            .map{
                case Array(score, label, app) => {
                    val appList = app.split(":").map{
                        x => (x, weight(hashingTF.indexOf(x)))
                    }
                        .sortBy(x => -Math.abs(x._2))
                        .map{
                            case (appName, w) => Seq(appName, w).mkString(":")
                        }
                    Seq(score, label, appList.mkString("|")).mkString("~")
                }
            }.saveAsTextFile("file:///E:\\inputData\\weight")

        /*val apps = ("酷狗音乐:百度贴吧:电信营业厅:上网导航:平安好车主:掌上电力:孕期提醒:qq空间:腾讯手游宝" +
            ":支付宝:oppo桌面:qq浏览器:美颜相机:全民k歌:91桌面:天天酷跑:乐视体育:豆果美食:暴风影音" +
            ":腾讯视频:手机京东:微博:兴趣部落:旺信:安全管家:笔记:手机淘宝").split(":")

        val res = apps.map{
            app => Seq(app, weight(hashingTF.indexOf(app))).mkString(":")
        }
        for (i <- res.indices){
            println(res(i))
        }*/

        sc.stop()
    }

}
