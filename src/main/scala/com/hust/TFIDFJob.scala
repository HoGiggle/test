package com.hust

import java.io.ObjectOutputStream

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/13.
  */
object TFIDFJob {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

//        val trainPath = "file:///E:\\data\\allTrain.txt"
//        val testPath = "file:///E:\\data\\newsTest.txt"
//        val stopPath = "file:///E:\\data\\stopWords"
        val trainPath = "/user/iflyrd/work/jjhu/ml/allTrain.txt"
        val testPath = "/user/iflyrd/work/jjhu/ml/newsTest.txt"
        val stopPath = "/user/iflyrd/falco/profile/ItemProfile/Resource/StopWords"

//        val model = SVMModel.load(sc, "file:///E:\\model\\svm")

        val stopWords = sc.broadcast(sc.textFile(stopPath).distinct().collect())

        val trainData = sc.textFile(trainPath).map(_.split(" "))
            .map {
                case Array(label, seq) => {
                    val arr = seq.split("~")
                    val afterStop = arr.map {
                        item => {
                            val Array(word, cls) = item.split(":")
                            word
                        }
                    }.filter(!stopWords.value.contains(_))
                    (label.toInt, afterStop)
                }
            }

        /*val testData = sc.textFile(testPath).map(_.split(" "))
            .map {
                case Array(label, seq) => {
                    val arr = seq.split("~")
                    val afterStop = arr.map {
                        item => {
                            val Array(word, cls) = item.split(":")
                            word
                        }
                    }.filter(!stopWords.value.contains(_))
                    (label.toInt, afterStop)
                }
            }*/

//        trainData.cache()
//        testData.cache()

        //train data tf-idf
        val tf = new HashingTF()
        val trainTf = trainData.map {
            case (label, words) => (label, tf.transform(words))
        }
        val idf = new IDF().fit(trainTf.values)
        /*val tfIDF = trainTf.mapValues(v => idf.transform(v))
        val tmp = tfIDF.map {
            case (label, vec) => LabeledPoint(label, vec.toSparse)
        }*/

        //segment results, (index, word)
        val wordsMapping = trainData.flatMap(_._2)
            .distinct()
            .map{
                word => Seq(tf.indexOf(word), word).mkString("~")
            }

        wordsMapping.coalesce(1).saveAsTextFile("/user/iflyrd/work/jjhu/ml/wordsMapping")

        /*val splits = tmp.randomSplit(Array(0.7, 0.3))
        val trainCol = splits(0).cache()
        val testCol = splits(1)

        /*//test data tf-idf
        val testCol = testData
            .map {
                case (label, words) => (label, tf.transform(words))
            }
            .map{
                case (label, vec) => LabeledPoint(label, idf.transform(vec).toDense)
            }*/

        //svm model run
        val svmAlg = new SVMWithSGD()
        svmAlg.optimizer
            .setNumIterations(100)
            .setRegParam(0.01d)
            .setUpdater(new SquaredL2Updater)
        val model = svmAlg.run(trainCol)

        // Clear the default threshold.
        model.clearThreshold()

        // Compute raw scores on the test set.
        val scoreAndLabels = testCol.map { point =>
            val score = model.predict(point.features)
            (score, point.label)
        }

        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        val auROC = metrics.areaUnderROC()
        println("Area under ROC = " + auROC)

        val acc = scoreAndLabels.filter{
            x => (x._1 > 0 && x._2 == 1.0) || (x._1 < 0 && x._2 == 0)
        }
            .count() * 1.0d  / scoreAndLabels.count()
        println("Acc = " + acc)*/

        //idf model save
        val hadoopConf = sc.hadoopConfiguration
        val fileSystem = FileSystem.get(hadoopConf)
        val path = new Path("/user/iflyrd/work/jjhu/ml/idf")
        val oos = new ObjectOutputStream(new FSDataOutputStream(fileSystem.create(path)))
        oos.writeObject(idf)
        oos.close

//        model.save(sc, "file:///E:\\model\\svm")
//        model.save(sc, "/user/iflyrd/work/jjhu/ml/svmAll")
//        trainCol.unpersist()
        sc.stop()
    }
}
