package com.expriment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/23.
  */
object KmeansTest {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
        val sc = new SparkContext(conf)

        //train data
        val trainData = sc.textFile("file:///E:\\kmeans_train.txt")
            .map(line => Vectors.dense(line.split(",")
            .map(_.trim)
            .filter(!"".equals(_))
            .map(_.toDouble))).cache()

        // Cluster the data into two classes using KMeans
        val numClusters = 8
        val numIterations = 20
        val clusters = KMeans.train(trainData, numClusters, numIterations)

        var clusterIndex = 0
        println("Cluster Centers Information Overview:")
        clusters.clusterCenters.foreach(
            x => {

                println("Center Point of Cluster " + clusterIndex + ":")

                println(x)
                clusterIndex += 1
            })

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        val WSSSE = clusters.computeCost(trainData)
        println("Within Set Sum of Squared Errors = " + WSSSE)

        //test data
        val testData = sc.textFile("file:///E:\\kmeans_test.txt")
            .map(line => Vectors.dense(line.split(",")
                .map(_.trim)
                .filter(!"".equals(_))
                .map(_.toDouble))).cache()

        testData.collect().foreach(testDataLine => {
            val predictedClusterIndex:
            Int = clusters.predict(testDataLine)

            println("The data " + testDataLine.toString + " belongs to cluster " +
                predictedClusterIndex)
        })

        println("Spark MLlib K-means clustering test finished.")

//        // Save and load model
//        clusters.save(sc, "myModelPath")
//        val sameModel = KMeansModel.load(sc, "myModelPath")

        sc.stop()
    }
}
