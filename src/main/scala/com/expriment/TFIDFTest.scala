package com.expriment

import com.util.CommonService
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by jjhu on 2016/12/5.
  */
object TFIDFTest {
    def main(args: Array[String]): Unit = {
        val Array(useDate, df, minAppNum, maxAppNum, minFreq) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        /** *****************************************************************
          * app数量异常过滤
          */
        val in = s"/user/iflyrd/work/jjhu/sex/appName/$useDate"
        val stopListPath = "/user/iflyrd/work/jjhu/sex/dataClean/exceptApp.txt"
        val maleFillPath = "/user/iflyrd/work/jjhu/sex/dataClean/maleApp.txt"
        val femaleFillPath = "/user/iflyrd/work/jjhu/sex/dataClean/femaleApp.txt"

        //broadcast
        val stopList = sc.broadcast(sc.textFile(stopListPath).collect())
        val maleFill = sc.broadcast(sc.textFile(maleFillPath).collect())
        val femaleFill = sc.broadcast(sc.textFile(femaleFillPath).collect())

        //uid, pkg, appName, colDays, actDays, sex
//        val tmp = version_1(sc, in, stopList, minAppNum, maxAppNum)
        val tmp = version_2(sc, in, stopList, minAppNum, maxAppNum, minFreq)

        val splits = tmp.randomSplit(Array(0.7, 0.3))
        val trainData = splits(0)
        val testData = splits(1)

        val fillTrainData = trainData
            /*.map {
                case (sex, apps) => {
                    if (apps.length < minAppNum.toInt) {
                        //训练集特征填充
                        var fillApp = apps
                        if (sex == 0) {
                            val random = getRandomList(3, maleFill.value.length)
                            for (i <- random.indices){
                                fillApp = fillApp.+:(maleFill.value(random(i)))
                            }
                        } else {
                            val random = getRandomList(3, femaleFill.value.length)
                            for (i <- random.indices){
                                fillApp = fillApp.+:(femaleFill.value(random(i)))
                            }
                        }
                        (sex, fillApp.distinct)
                    } else {
                        (sex, apps)
                    }
                }
            }*/
            .cache()

        println("Males = " + fillTrainData.filter(_._1 == 0).count())
        println("Females = " + fillTrainData.filter(_._1 == 1).count())

        //训练集TF向量化
        val hashingTF = new HashingTF()
        val documents_train = fillTrainData.map {
            case (num, seq) =>
                val tf = hashingTF.transform(seq)
                (num, tf)
        }
        val idf = new IDF(minDocFreq = df.toInt).fit(documents_train.values)
        val num_idf_pairs_train = documents_train.mapValues(v => idf.transform(v))
        val trainCollection = num_idf_pairs_train.map {
            case (num, vector) =>
                LabeledPoint(num, vector.toSparse)
        }

        //测试集
        val testCol = testData
            .map {
                case (num, seq) =>
                    val tf = hashingTF.transform(seq)
                    (num, tf)
            }.mapValues(v => idf.transform(v)).map {
            case (num, vector) =>
                LabeledPoint(num, vector.toSparse)
        }

        //idf model & available data save
        trainCollection.coalesce(16).saveAsObjectFile(s"/user/iflyrd/work/jjhu/sex/trainFill_${df}_${minAppNum}_$maxAppNum")
        testCol.coalesce(16).saveAsObjectFile(s"/user/iflyrd/work/jjhu/sex/testFill_${df}_${minAppNum}_$maxAppNum")

        //tf-idf index appName映射
        CommonService.saveFeatureMapping(fillTrainData, "/user/iflyrd/work/jjhu/sex/dataClean/mapping")
        CommonService.saveIdfModel(sc, "/user/iflyrd/work/jjhu/sex/model/idf", idf)

        fillTrainData.unpersist()
        tmp.unpersist()
        sc.stop()
    }

    def version_2(sc: SparkContext,
                  in: String,
                  stopList: Broadcast[Array[String]],
                  minAppNum: String,
                  maxAppNum: String,
                  minFreq: String): RDD[(Int, Array[String])] = {
        val res = sc.textFile(in).map(_.split("~"))
            .map {
                case Array(uid, appName, date, colDays, actDays, sex) =>
                    val freq = CommonService.formatDiv(actDays.toInt, colDays.toInt, 2)
                    if (sex == "MALE") {
                        ((uid, appName, 0), (date, freq))
                    } else {
                        ((uid, appName, 1), (date, freq))
                    }
            }
            .reduceByKey {
                //采集日期取最新日期
                case (x, y) => if (x._1 > y._1) x else y
            }
            .map { x => ((x._1._1, x._1._3), (x._1._2, x._2._2.toFloat, 1)) } //((uid, sex), (appName, freq, times))
            .reduceByKey {
                case (x, y) => (Seq(x._1, y._1).mkString("~"), x._2 + y._2, x._3 + y._3)
            }
            .filter{
                //用户app平均使用频率过滤
                case ((uid, sex), (appName, freq, times)) => (freq / times) >= minFreq.toFloat
            }
            .map {
                case ((uid, sex), (apps, freq, times)) => {
                    val appArr = apps.split("~")
//                    (sex, appArr.diff(stopList.value)) //无意义app过滤, 停用app
                    (sex, appArr)
                }
            }
            .filter {
                //app安装数过滤
                case (sex, apps) => (apps.length >= minAppNum.toInt) && (apps.length <= maxAppNum.toInt)
            }

        res
    }

    def version_1(sc: SparkContext,
                  in: String,
                  stopList: Broadcast[Array[String]],
                  minAppNum: String,
                  maxAppNum: String): RDD[(Int, Array[String])] = {
        val res = sc.textFile(in).map(_.split("~"))
            .map {
                case Array(uid, appName, date, colDays, actDays, sex) =>
                    if (sex == "MALE") {
                        ((uid, appName, 0), date)
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
                    (sex, appArr.diff(stopList.value)) //无意义app过滤, 停用app
                }
            }
            .filter {
                //app安装数据过滤
                case (sex, apps) => (apps.length >= minAppNum.toInt) && (apps.length <= maxAppNum.toInt)
            }

        res
    }

    def getRandomList(n: Int, len: Int): List[Int] = {
        var resultList: List[Int] = Nil
        while (resultList.length < n) {
            val randomNum = (new Random).nextInt(len)
            if (!resultList.contains(randomNum)) {
                resultList = resultList ::: List(randomNum)
            }
        }
        resultList
    }
}