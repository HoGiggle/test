package com.expriment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/6.
  */
object LRTest {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val Array(iter) = args
//        val model = LogisticRegressionModel.load(sc, "/user/iflyrd/work/jjhu/sex/LR")


        // Load training data in LIBSVM format.
        val training = sc.objectFile[LabeledPoint]("/user/iflyrd/work/jjhu/sex/trainCol").cache()
        val test = sc.objectFile[LabeledPoint]("/user/iflyrd/work/jjhu/sex/testCol").cache()

        //Run training algorithm to build the model
        val model = new LogisticRegressionWithLBFGS()
            .setNumClasses(2)
            .run(training)

        // Compute raw scores on the test set.
        val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
            val prediction = model.predict(features)
            (prediction, label)
        }

        // Get evaluation metrics.
        val metrics = new MulticlassMetrics(predictionAndLabels)
        val precision = metrics.precision
        println("Precision = " + precision)
        println("Weights = " + model.weights.toSparse)
        println("Features = " + model.numFeatures)
        println("Intercept = " + model.intercept)

        // Save and load model
//        model.save(sc, "/user/iflyrd/work/jjhu/sex/LR")
//        test.unpersist()
        sc.stop()
    }
}
