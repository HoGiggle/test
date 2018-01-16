package com.expriment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, SVMModel}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/21.
  */
object SVMTest_2 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val Array(df) = args

        val model = SVMModel.load(sc, s"/user/iflyrd/work/jjhu/sex/svm")
        val test1 = sc.objectFile[LabeledPoint](s"/user/iflyrd/work/jjhu/sex/testFill_$df")

        val mapping = sc.broadcast(sc.textFile("/user/iflyrd/work/jjhu/sex/dataClean/mapping").map(_.split("~"))
            .map{
                case Array(index, appName) => (index.toInt, appName)
            }
            .collect().toMap)

        val test = test1.sample(false, 0.0001d, System.currentTimeMillis().toInt)
            .cache()
        println("sample = " + test.count())
        println("total = " + test1.count())

        // Compute raw scores on the test set.
        val scoreAndLabels = test.map { point =>
            val score = model.predict(point.features)
            (score, point.label, point.features.toSparse)
        }.cache()
        scoreAndLabels
            .map{
                case (dis, label, feature) => {
                    val apps = feature.indices.map(index => mapping.value(index))
                    Seq(dis, label, apps.mkString(":")).mkString("~")
                }
            }
            .coalesce(1)
            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/SVMSample")

        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(scoreAndLabels.map(x => (x._1, x._2)))
        val auROC = metrics.areaUnderROC()
        println("Area under ROC = " + auROC)

        val acc = scoreAndLabels.filter{
            x => (x._1 > 0 && x._2 == 1.0) || (x._1 < 0 && x._2 == 0)
        }
            .count() * 1.0d  / scoreAndLabels.count()
        println("Acc = " + acc)

        /**
          * LR model
          */
        //Run training algorithm to build the model
        val LRModel = LogisticRegressionModel.load(sc, "/user/iflyrd/work/jjhu/sex/lr")

        // Compute raw scores on the test set.
        val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
            val prediction = LRModel.predict(features)
            (prediction, label, features.toSparse)
        }.cache()

        predictionAndLabels.map{
            case (dis, label, feature) => {
                val apps = feature.indices.map(index => mapping.value(index))
                Seq(dis, label, apps.mkString(":")).mkString("~")
            }
        }
            .coalesce(1)
            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/LRSample")

        // Get evaluation metrics.
        val LRBM = new BinaryClassificationMetrics(predictionAndLabels.map(x => (x._1, x._2)))
        val LRRoc = LRBM.areaUnderROC()
        println("LR Auc = " + LRRoc)

        val LRMetrics = new MulticlassMetrics(predictionAndLabels.map(x => (x._1, x._2)))
        println("LR Precision = " + LRMetrics.precision)
        println("LR Recall = " + LRMetrics.recall)

        // Save and load model
        test.unpersist()
        scoreAndLabels.unpersist()
        sc.stop()
    }
}
