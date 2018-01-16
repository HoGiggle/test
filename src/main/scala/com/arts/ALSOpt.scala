package com.arts

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/11.
  */
object ALSOpt {
    def main(args: Array[String]): Unit = {
        val Array(confPath, runDate, dates, days, runType, rank, iter) = args
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val config = getConfig(confPath).getConfig("job")
        val sc = new SparkContext(conf)
        initParam(config)

        val modelType = Seq(days, rank, iter).mkString("_")

        if (runType == "0") {
            val inPath = s"/user/iflyrd/falco/UserActions/$dates/"
            val uidIndexPath = s"/user/iflyrd/work/jjhu/als/$days/uidIndex"
            val docIndexPath = s"/user/iflyrd/work/jjhu/als/$days/docIndex"
            val uidDocMap = s"/user/iflyrd/work/jjhu/als/$days/uidDocMap"
            val modelPath = s"/user/iflyrd/work/jjhu/als/$days/model/$modelType"

            //用户行为数据
            val actLog = sc.textFile(inPath)   //actLog需要cache吗？
                .map(_.split("~", -1))
                .filter(t => (t.length >= 4) && (t(3).toDouble >= 0))

            val qualUser = actLog.map(t => (t(1), t(0)))
                .distinct()
                .map(t => (t._1, 1))
                .reduceByKey(_ + _)
                .filter(t => t._2 >= activeDays)//活跃天数过滤

            val initData = actLog.map(t => ((t(1), t(2)), t(3).toDouble))
                .reduceByKey((x, y) => if (x > y) x else y)
                .map {
                    case ((uid, docId), score) => (uid, (docId, score))
                }
                .join(qualUser)
                .map{
                    case (uid, ((docId, score), count)) => (uid, docId, score)
                }

            initData.cache()

            val users = initData.map(_._1).distinct().collect()
            val userSeq = for (i <- 0 until users.length) yield (users(i), i)
            val userIndex = sc.parallelize(userSeq)

            val items = initData.map(_._2).distinct().collect()
            val itemSeq = for (i <- 0 until items.length) yield (items(i), i)
            val itemIndex = sc.parallelize(itemSeq)

            //训练数据
            val trainData = initData.map(x => (x._1, (x._2, x._3)))
                .join(userIndex)
                .map(x => (x._2._1._1, (x._2._2, x._2._1._2))) //(itemid, (user_index, score)
                .join(itemIndex)
                .map(x => (x._2._1._1, x._2._2, x._2._1._2))
                .map(x => Rating(x._1, x._2, x._3))

            userIndex.cache()
            itemIndex.cache()
            trainData.cache()

            userIndex.map(x => Seq(x._1, x._2).mkString("~")).saveAsTextFile(uidIndexPath)
            itemIndex.map(x => Seq(x._1, x._2).mkString("~")).saveAsTextFile(docIndexPath)
            trainData.saveAsObjectFile(uidDocMap)

            //训练ALS模型
//            val model = trainModel(trainType, trainData, featureNum, iterationNum, lambda, alpha)
            val model = trainModel(trainType, trainData, rank.toInt, iter.toInt, lambda, alpha)
            model.save(sc, modelPath)

            /* //预测topK
             val recommTopK = predictTopK(model, topK)
                 .map(x => (x._1, (x._2, x._3)))
                 .join(userIndex.map(x => (x._2, x._1)))
                 .map(x => (x._2._1._1, (x._2._2, x._2._1._2)))
                 .join(itemIndex.map(x => (x._2, x._1)))
                 .map(x => (x._2._1._1, x._2._2, x._2._1._2))

             saveResult(recommTopK, resultOutput, runDate, resultOutputPartition)*/

            trainData.unpersist()
            itemIndex.unpersist()
            userIndex.unpersist()
        } else {
            val trainData = sc.objectFile[Rating](s"/user/iflyrd/work/jjhu/als/$days/uidDocMap")
            val modelPath = s"/user/iflyrd/work/jjhu/als/$days/model/$modelType"

            //训练ALS模型
//            val model = trainModel(trainType, trainData, featureNum, iterationNum, lambda, alpha)
            val model = trainModel(trainType, trainData, rank.toInt, iter.toInt, lambda, alpha)
            model.save(sc, modelPath)
        }

        sc.stop
    }

    def getConfig(confPath:String): com.typesafe.config.Config ={
        val confFile = ConfigFactory.parseFile(new File(confPath))
        ConfigFactory.load(confFile)
    }

    /**
      * 存储TopK结果
      * @param result
      * @param resultOutput
      * @param cdate
      * @param resultOutputPartition
      */
    def saveResult(result: RDD[(String, String, Double)],
                   resultOutput: String,
                   cdate: String,
                   resultOutputPartition: Int): Unit = {
        val output = Array(resultOutput, cdate).mkString("/")
        result.map {
            case (uid, nid, score) => Array(uid, nid, score.toString).mkString("~")
        }
            .coalesce(resultOutputPartition, true).saveAsTextFile(output)
    }

    def predictTopK(model: MatrixFactorizationModel,
                    topK: Int): RDD[(Int, Int, Double)] = {
        model.recommendProductsForUsers(topK)
            .flatMap {
                case (uid, ratings) => ratings.map {
                    case Rating(uid, nid, score) => (uid, nid, score)
                }
            }
    }

    def saveUserFeature(model: MatrixFactorizationModel,
                        userIndex: RDD[(String, Int)],
                        userFeatureOutput: String,
                        userFeatureOutputPartition: Int,
                        cDate: String): Unit = {
        val output = Array(userFeatureOutput, cDate).mkString("/")
        val userFeature = model.userFeatures
            .join(userIndex.map {
                case (uid, index) => (index, uid)
            })
            .map {
                case (index, (featureArr, uid)) => (uid, Vectors.dense(featureArr))
            }
        userFeature.coalesce(userFeatureOutputPartition, true)
            .saveAsObjectFile(output)
    }

    def trainModel(trainType: String,
                   trainData: RDD[Rating],
                   featureNum: Int,
                   iterationNum: Int,
                   lambda: Double,
                   alpha: Double): MatrixFactorizationModel = {
        trainType match {
            case "explicit" => ALS.train(trainData, featureNum, iterationNum, lambda)
            case "implicit" => ALS.trainImplicit(trainData, featureNum, iterationNum, lambda, alpha)
        }
    }

    /**
      * 加载HDFS上的数据
      *
      * @param sc
      * @param hdfsInput
      * @return
      */
    def loadHDFSData(sc: SparkContext,
                     hdfsInput: String): RDD[(String, String, Double)] = {
        sc.textFile(hdfsInput)
            .map(_.split("~", -1))
            .filter(t => (t.length >= 4) && (t(3).toDouble >= 0))
            .map(t => ((t(1), t(2)), t(3).toDouble))
            .reduceByKey((x, y) => if (x > y) x else y)
            .map {
                case ((uid, docId), score) => (uid, docId, score)
            }
    }

    /**
      * 初始化参数
      * @param config
      */
    def initParam(config: Config): Unit = {
        trainType = config.getString("als.train_type")
        featureNum = config.getInt("als.feature_num")
        iterationNum = config.getInt("als.iteration_num")
        lambda = config.getDouble("als.lambda")
        alpha = config.getDouble("als.alpha")
        topK = config.getInt("als.top_k")

        hbaseTable = config.getString("hbase.table")
        zookeeperQuorum = config.getString("hbase.zookeeperQuorum")
        hbaseColumn = config.getString("hbase.column")

        hdfsInput = config.getString("hdfs.input")
        userFeatureOutput = config.getString("hdfs.user_feature_output")
        userFeatureOutputPartition = config.getInt("hdfs.user_feature_output_partition")
        resultOutput = config.getString("hdfs.result_output")
        resultOutputPartition = config.getInt("hdfs.result_output_partition")

        log4jInput = config.getString("log4j_input")
        activeDays = config.getInt("activeDays")
    }

    @transient private var trainType: String = _
    @transient private var featureNum: Int = _
    @transient private var iterationNum: Int = _
    @transient private var lambda: Double = _
    @transient private var alpha: Double = _
    @transient private var topK: Int = _
    @transient private var hdfsInput: String = _
    @transient private var hbaseTable: String = _
    @transient private var zookeeperQuorum: String = _
    @transient private var hbaseColumn: String = _
    @transient private var userFeatureOutput: String = _
    @transient private var userFeatureOutputPartition: Int = _
    @transient private var resultOutput: String = _
    @transient private var resultOutputPartition: Int = _
    @transient private var log4jInput: String = _
    @transient private var activeDays: Int = _
}

