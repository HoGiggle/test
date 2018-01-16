package com.hust

import java.io.{FileInputStream, ObjectInputStream}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.feature.{HashingTF, IDFModel}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/14.
  */
object Predict {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
        val sc = new SparkContext(conf)

        val predPath = "file:///E:\\data\\predict.txt"
        val model = SVMModel.load(sc, "file:///E:\\model\\svmAll")
        val threshold = 1.0d

        val tf = new HashingTF()
        val ois = new ObjectInputStream(new FileInputStream("E:\\model\\idf"))
        val idf = ois.readObject.asInstanceOf[IDFModel]

        val predData = sc.textFile(predPath).map(_.split(" "))
            .map {
                case Array(label, seq) => {
                    val arr = seq.split("~")
                    val afterStop = arr.map {
                        item => {
                            val Array(word, cls) = item.split(":")
                            word
                        }
                    }
                    (label, afterStop)
                }
            }
            .map {
                case (label, words) => (label, tf.transform(words))
            }
            .map {
                case (label, vec) => (label, idf.transform(vec).toSparse)
            }

        //        wordsMapping.coalesce(1).saveAsTextFile("/user/iflyrd/work/jjhu/ml/wordsMapping")

        // predict
        val scoreAndLabels = predData
            .map {
                case (label, features) => {
                    val score = model.predict(features)
                    if (Math.abs(score) >= threshold) {
                        if (score > 0) {
                            (label, 1)
                        } else {
                            (label, 0)
                        }
                    } else {
                        (label, -1)
                    }
                }
            }

        scoreAndLabels.map(x => Seq(x._1, x._2).mkString("~"))
            .coalesce(1).saveAsTextFile("file:///E:\\data\\result")

        sc.stop()
    }
}
