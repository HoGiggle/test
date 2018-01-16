package com.kuyin.strategy

import com.kuyin.structure.{BehaviorLevel, RecommendALG}
import com.typesafe.config.Config
import com.util.{CommonService, HdfsConfHelper, LocalConfHelper}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * als用户行为矩阵分解
  * 1、过去三天内用户试听行为作为als输入矩阵
  */
object ALSStrategy extends RecommendUtils{
  def main(args: Array[String]) {
    val Array(confPath, hdfsRange, cDate, mode) = args
    val config = if (mode == "cluster") {
      HdfsConfHelper.getConfig(confPath).getConfig("job")
    } else {
      LocalConfHelper.getConfig(confPath).getConfig("job")
    }
    //初始化参数
    initParam(config)
    val conf = new SparkConf().setAppName(this.getClass.getName + "_" + cDate)
    val sc = new SparkContext(conf)
    //设置checkpoint
    sc.setCheckpointDir(checkPointDir)
    val hdfsData = loadHDFSData(sc, behaviorPath, hdfsRange)
    val dayGrayUser = loadGrayUser(sc, grayPath, cDate)
    val rateData = prepareTrainData(hdfsData, dayGrayUser)
    rateData.cache()

    println("all audi size = " + rateData.count())

    //item过滤, 推荐铃声小库 todo 铃声试听人次信息表, 3、7天
    /*//测试用例
    val testDataPath = s"/user/iflyrd/work/jjhu/kuyin/audi/$cDate"
    val testData = sc.textFile(testDataPath).map(_.split("~"))
      .map{
        case Array(date, uid, itemId, autoFlag, value) => {
          (uid, itemId, value.toDouble, autoFlag)
        }
      }.cache()

    val selfAudi = distinctBehavior(testData.filter(_._4 == "0").map(x => (x._1 + "_0", x._2, x._3)))
    val allAudi = distinctBehavior(testData.map(x => (x._1 + "_1", x._2, x._3)))*/

    //item 编码
    val items = rateData.map{
      case (uid, itemId, score) => (itemId, 1)
    }
      .reduceByKey(_ + _)
      .filter(_._2 >= config.getInt("itemFloorLimit"))
      .map{
        case (itemId, cnt) => itemId
      }
//      .union(selfAudi.map(x => x._2))
//      .union(allAudi.map(x => x._2))
      .distinct()
      .collect()
    val itemSeq = for (i <- items.indices) yield (items(i), i)
    val itemIndex = sc.parallelize(itemSeq)
    println("local item length = " + items.length)

    //user 编码
    val users = rateData.map(_._1).distinct().collect()
    val userSeq = for (i <- users.indices) yield (users(i), i + 10) //留前10位编码测试用
//    userSeq = userSeq.+:(("5000000000159456023_0", 0))
//    userSeq = userSeq.+:(("5000000000159456023_1", 1))
    val userIndex = sc.parallelize(userSeq)
    println("local user length = " + users.length)

    /** item表相对较小, 先join小表 **/
    val trainData = rateData.map{
        case (uid, itemId, score) => (itemId, (uid, score))
      }
      .join(itemIndex)
      .map{
        case (itemId, ((uid, score), iIndex)) => (uid, (iIndex, score))
      }
      .join(userIndex)
      .map{
        case (uid, ((iIndex, score), uIndex)) => Rating(uIndex, iIndex, score)
      }

    itemIndex.cache()
    userIndex.cache()
    trainData.cache()
    rateData.unpersist()

    //训练ALS模型
    val model = trainModel(trainType, trainData, featureNum, iterationNum, lambda, alpha)

    /**模型参数调整**/
//    println("model user size = " + model.userFeatures.count())
//    println("model item size = " + model.productFeatures.count())
//    paramAdjust(trainData, sampleSize)
//    val rmse = computeRmse(model, trainData, sampleSize)
//    println("RMSE = " + rmse)
//    model.save(sc, s"/user/iflyrd/kuyin/strategy/ALS/model/${cDate}_${featureNum}_${iterationNum}")

    //预测topK
    val recommTopK = predictTopK(model, topK)
      .map(x => (x._1, (x._2, x._3)))
      .join(userIndex.map(x => (x._2, x._1)))
      .map(x => (x._2._1._1, (x._2._2, x._2._1._2)))
      .join(itemIndex.map(x => (x._2, x._1)))
      .map(x => (x._2._1._1, x._2._2, x._2._1._2))
      .cache()

    /*//离线结果验证
    recommTopK.filter{
      case (uid, itemId, score) => (uid == "5000000000159456023_0") || (uid == "5000000000159456023_1")
    }
      .map{
        case (uid, itemId, score) => (itemId, (uid, score))
      }
      .join(validItems)
      .map{
        case (itemId, ((uid, score), map)) =>
          (Seq(uid, itemId, Bytes.toString(map.getOrElse("cf:ringName", Array[Byte]())), score).mkString("~"), score)
      }
        .sortBy(_._2)
      .collect()
      .foreach(println)*/

    //存储用户特征矩阵 && 预测结果
    saveUserFeature(model, userIndex, userFeatureOutput, userFeatureOutputPartition, cDate)
    saveItemFeature(model, itemIndex, itemFeatureOutput, itemFeatureOutputPartition, cDate)
    saveRecommHDFSWithScore(recommTopK, resultOutput, cDate, resultOutputPartition, RecommendALG.ALS.toString)
    saveRecommHBASE(sc, hbaseTable, zookeeperQuorum, hbaseColumn, recommTopK.map(x => (x._1, x._2)), cDate, RecommendALG.ALS)


    trainData.unpersist()
    itemIndex.unpersist()
    userIndex.unpersist()
    recommTopK.unpersist()
    sc.stop
  }


  def paramAdjust(trainData: RDD[Rating],
                  sampleSize: Long): Unit ={
    //训练不同参数下的模型，并在校验集中验证，获取最佳参数下的模型
    val ranks = List(200, 250, 300)
    val lambdas = List(0.01)
    val numIters = List(15, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val start = System.currentTimeMillis()

      val model = trainModel(trainType, trainData, rank, numIter, lambda, alpha)
      val validationRmse = computeRmse(model, trainData, sampleSize)

      val end = System.currentTimeMillis()
      println("RMSE = " + validationRmse + ", rank = "
        + rank + ", lambda = " + lambda + ", iterator = " + numIter + s", time = ${end - start}/s" )

      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    println("Best info : RMSE = " + bestValidationRmse + ", rank = " + bestRank +
      ", lambda = " + bestLambda + ", iterator = " + bestNumIter)
  }

  /** 校验集预测数据和实际数据之间的均方根误差 **/
  def computeRmse(model: MatrixFactorizationModel,
                  data: RDD[Rating],
                  n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map{ x =>((x.user, x.product), x.rating)}
      .join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map( x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  def distinctBehavior(hdfsData: RDD[(String, String, Double)]): RDD[(String, String, Double)] = {

    hdfsData.map {
      case (uid, nid, point) => ((uid, nid), point)
    }
      .reduceByKey((a, b) => if (a > b) a else b)
      .map {
        case ((uid, nid), point) => (uid, nid, point)
      }
  }

  /**
    * 获得灰度用户的行为矩阵
    *
    * @param hdfsData
    * @return
    */
  def prepareTrainData(hdfsData: RDD[(String, String, Double)],
                       grayData: RDD[(String, String)]): RDD[(String, String, Double)] = {

    val middle = hdfsData.map {
      case (uid, nid, point) => ((uid, nid), point)
    }
      .reduceByKey((a, b) => if (a > b) a else b)
      .map {
        case ((uid, nid), point) => (uid, (nid, point))
      }

    middle.join(grayData)
      .map{
        case (uid, ((itemId, score), grayDate)) => (uid, itemId, score)
      }
  }

  /**
    * 加载HDFS上的数据
    *
    * @param sc
    * @param behaviorPath
    * @param hdfsRange
    * @return
    */
  def loadHDFSData(sc: SparkContext,
                   behaviorPath: String,
                   hdfsRange: String): RDD[(String, String, Double)] = {
    val input = Array(behaviorPath, hdfsRange).mkString("/")
    sc.textFile(input)
      .map(_.split("~", -1))
      .filter(t => t.length >= 5)
      .map{
        case Array(date, uid, itemId, autoFlag, score) => (uid, itemId, score.toDouble)
      }
      .filter {
        case (uid, nid, score) => score >= BehaviorLevel.Audition  //目前只考虑试听行为, 展示行为迭代
      }
  }



  /**
    * 训练ALS模型
    *
    * @param trainType
    * @param trainData
    * @param featureNum
    * @param iterationNum
    * @param lambda
    * @param alpha
    * @return
    */
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
    * 预测TopK结果
    *
    * @param model
    * @param topK
    * @return
    */
  def predictTopK(model: MatrixFactorizationModel,
                  topK: Int): RDD[(Int, Int, String)] = {
//    val tmp = EnhancedMatrixFactorizationModel.recommendProductsForUsers(model, topK, 0.0d, 70, 6)
//    tmp.flatMap{
//      case (uid, arr) => arr.map{
//        case (itemId, score) => (uid, itemId, score)
//      }
//    }
    model.recommendProductsForUsers(topK)
      .flatMap {
        case (uid, ratings) => ratings.map {
          case Rating(uid1, nid, score) => (uid, nid, CommonService.formatDouble(score, 3))
        }
      }
  }


  /**
    * 存储用户特征矩阵
    *
    * @param model
    * @param userIndex
    * @param userFeatureOutput
    * @param userFeatureOutputPartition
    * @param cDate
    */
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


  def saveItemFeature(model: MatrixFactorizationModel,
                      itemIndex: RDD[(String, Int)],
                      itemFeatureOutput: String,
                      partition: Int,
                      cDate: String): Unit = {
    val output = Array(itemFeatureOutput, cDate).mkString("/")
    val itemFeature = model.productFeatures
      .join(itemIndex.map {
        case (uid, index) => (index, uid)
      })
      .map {
        case (index, (featureArr, uid)) => (uid, Vectors.dense(featureArr))
      }
    itemFeature.coalesce(partition, true)
      .saveAsObjectFile(output)
  }


  /**
    * 初始化参数
    *
    * @param config
    */
  def initParam(config: Config): Unit = {
    checkPointDir = config.getString("checkpoint_dir")
    trainType = config.getString("train_type")
    featureNum = config.getInt("feature_num")
    iterationNum = config.getInt("iteration_num")
    lambda = config.getDouble("lambda")
    alpha = config.getDouble("alpha")
    topK = config.getInt("top_k")
    behaviorPath = config.getString("hdfs_input")
    hbaseTable = config.getString("hbase_table")
    zookeeperQuorum = config.getString("zookeeper_quorum")
    hbaseColumn = config.getString("hbase_column")
    userFeatureOutput = config.getString("user_feature_output")
    userFeatureOutputPartition = config.getInt("user_feature_output_partition")
    itemFeatureOutput = config.getString("item_feature_output")
    itemFeatureOutputPartition = config.getInt("item_feature_output_partition")
    resultOutput = config.getString("result_output")
    resultOutputPartition = config.getInt("result_output_partition")
    log4jInput = config.getString("log4j_input")

    userUpperLimit = config.getInt("userUpperLimit")
    userFloorLimit = config.getInt("userFloorLimit")
    itemFloorLimit = config.getInt("itemFloorLimit")
    grayPath = config.getString("grayPath")
  }

  @transient private var checkPointDir             : String = _
  @transient private var trainType                 : String = _
  @transient private var featureNum                : Int    = _
  @transient private var iterationNum              : Int    = _
  @transient private var lambda                    : Double = _
  @transient private var alpha                     : Double = _
  @transient private var topK                      : Int    = _
  @transient private var behaviorPath                 : String = _
  @transient private var hbaseTable                : String = _
  @transient private var zookeeperQuorum           : String = _
  @transient private var hbaseColumn               : String = _
  @transient private var userFeatureOutput         : String = _
  @transient private var userFeatureOutputPartition: Int    = _
  @transient private var itemFeatureOutput         : String = _
  @transient private var itemFeatureOutputPartition: Int    = _
  @transient private var resultOutput              : String = _
  @transient private var resultOutputPartition     : Int    = _
  @transient private var log4jInput                : String = _

  @transient private var userUpperLimit: Int = _
  @transient private var userFloorLimit: Int = _
  @transient private var itemFloorLimit: Int = _
  @transient private var grayPath: String = _
}
