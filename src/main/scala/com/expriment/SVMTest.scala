package com.expriment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/1.
  */
object SVMTest {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val Array(df,iter,l2) = args

        // Load training data in LIBSVM format.
//        val model = SVMModel.load(sc, s"/user/iflyrd/work/jjhu/sex/svm/$iter")
        val training = sc.objectFile[LabeledPoint](s"/user/iflyrd/work/jjhu/sex/trainFill_$df").cache()
        val test = sc.objectFile[LabeledPoint](s"/user/iflyrd/work/jjhu/sex/testFill_$df").cache()
        val mapping = sc.broadcast(sc.textFile("/user/iflyrd/work/jjhu/sex/dataClean/mapping").map(_.split("~"))
            .map{
                case Array(index, appName) => (index.toInt, appName)
            }
            .collect().toMap)

        /**
          * svm model
          */
        // Run training algorithm to build the model
        val numIterations = iter.toInt
        val svmAlg = new SVMWithSGD()
        svmAlg.optimizer
            .setNumIterations(numIterations)
            .setRegParam(l2.toDouble)
            .setUpdater(new SquaredL2Updater)
        val model = svmAlg.run(training)

        // Clear the default threshold.
        model.clearThreshold()

        // Compute raw scores on the test set.
        val scoreAndLabels = test.map { point =>
            val score = model.predict(point.features)
            (score, point.label, point.features.toSparse)
        }.cache()
        scoreAndLabels.map {
            case (score, label, feature) => {
                if (label == 0d) {
                    (score * (-1), label, feature)
                } else {
                    (score, label, feature)
                }
            }
        }
            .filter(_._1 < 0)
            .sortBy(_._1)
            .map{
                case (dis, label, feature) => {
                    val apps = feature.indices.map(index => mapping.value(index))
                    Seq(dis, label, apps.mkString(":")).mkString("~")
                }
            }
            .coalesce(1)
            .saveAsTextFile("/user/iflyrd/work/jjhu/sex/badcase")

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
        val LRModel = new LogisticRegressionWithLBFGS()
            .setNumClasses(2)
            .run(training)

        // Compute raw scores on the test set.
        val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
            val prediction = LRModel.predict(features)
            (prediction, label)
        }

        // Get evaluation metrics.
        val LRBM = new BinaryClassificationMetrics(predictionAndLabels.map(x => (x._1, x._2)))
        val LRRoc = LRBM.areaUnderROC()
        println("LR Auc = " + LRRoc)

        val LRMetrics = new MulticlassMetrics(predictionAndLabels)
        println("LR Precision = " + LRMetrics.precision)
        println("LR Recall = " + LRMetrics.recall)

        val tmp = LRModel.weights.toSparse.indices.zip(LRModel.weights.toSparse.values)
        tmp.sortBy(-_._2).slice(0, 40)
            .map{
                case (index, weight) => (mapping.value(index), weight)
            }
            .foreach(println)

        //model and result save
        predictionAndLabels.map(x => Seq(x._1, x._2).mkString("~")).coalesce(1).saveAsTextFile("/user/iflyrd/work/jjhu/sex/lrResult")
        scoreAndLabels.map(x => Seq(x._1, x._2).mkString("~")).coalesce(1).saveAsTextFile("/user/iflyrd/work/jjhu/sex/svmResult")
        model.save(sc, "/user/iflyrd/work/jjhu/sex/svm")
        LRModel.save(sc, "/user/iflyrd/work/jjhu/sex/lr")

        // Save and load model
        training.unpersist()
        test.unpersist()
        scoreAndLabels.unpersist()
        sc.stop()
    }
}
