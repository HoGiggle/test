package com.expriment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/22.
  */
object LDATest {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
        val sc = new SparkContext(conf)

        // Load and parse the data
        val data = sc.textFile("file:///E:\\sample_lda_data.txt")
        val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
        // Index documents with unique IDs
        val corpus = parsedData.zipWithIndex.map(_.swap).cache()

        // Cluster the documents into three topics using LDA
        val ldaModel = new LDA().setK(3).run(corpus)


        val topK = 3
        // Output topics. Each is a distribution over words (matching word count vectors)
        println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
        val topics = ldaModel.topicsMatrix
        val tmp = ldaModel.describeTopics(topK)
        println("The topics described by their top-weighted terms:")
        for (topic <- Range(0, 3)){
            print("Topic " + topic + ":")
            for (i <- Range(0, topK)) {
                val (terms, score) = tmp(topic)
                print(" " + terms(i) + " " + score(i))
            }
            println()
        }


        for (topic <- Range(0, 3)) {
            print("Topic " + topic + ":")
            for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
            println()
        }

        // Save and load model.
//        ldaModel.save(sc, "file:///E:\\LDAModel")
//        val sameModel = DistributedLDAModel.load(sc,
//            "target/org/apache/spark/LatentDirichletAllocationExample/LDAModel")

        sc.stop()
    }
}
