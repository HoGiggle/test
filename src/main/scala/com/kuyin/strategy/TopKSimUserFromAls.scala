package com.kuyin.strategy

import java.text.NumberFormat

import com.github.fommil.netlib.{F2jBLAS, BLAS => NetlibBLAS}
import com.typesafe.config.Config
import com.util.{CommonService, HBaseUtil, HdfsConfHelper, LocalConfHelper}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * 根据ALS算法计算的用户模型,裸算用户相似度,并取相似度Top20的用户存入Hbase表'iflyrd:FalcoTopKSimUser'
  * <ul>
  * <li><font color="yellow">_f2jBLAS</font> NetlibBLAS对象,用于计算两个稠密向量的点积 </li>
  * <li><font color="yellow">hbaseHost</font> 线上hbase host配置 </li>
  * <li><font color="yellow">similarityHbaseTable</font> topK结果hbase表名 </li>
  * <li><font color="yellow">similarityHbaseCol</font> topK结果'列簇:修饰符' </li>
  * <li><font color="yellow">simScoreThreshold</font> 最低相似度,用于过滤相似度太低的用户群体 </li>
  * <li><font color="yellow">topK</font> topK大小 </li>
  * </ul>
  * Created by jjhu on 2016/5/17.
  */

object TopKSimUserFromAls {
  def main(args: Array[String]) {
    val Array(confPath, date, mode) = args
    val config = if (mode == "cluster") {
      HdfsConfHelper.getConfig(confPath).getConfig("job")
    } else {
      LocalConfHelper.getConfig(confPath).getConfig("job")
    }

    PropertyConfigurator.configure(config.getString("log4j"))
    initParam(config)
    val userFeaturesInput = s"/user/iflyrd/kuyin/strategy/ALS/UserFeature/$date"
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getName))
    sc.setCheckpointDir(checkPointDir)

    /** *******************************************************************
      * 加载用户特征向量, 裸算用户cos相似度, 取topK并存入hbase
      */
    val userFeatures = loadUserFeaturesFromHdfs(sc, userFeaturesInput, parallelism)
    val bUserFeatures = sc.broadcast(userFeatures.collect())
    println("user length = " + bUserFeatures.value.length)

    //用户特征矩阵自身笛卡尔积, 计算量 = userLen * userLen * rank (userLen三天规模在28w, rank = 200, userLen >> rank)
    //考虑是不是可以根据用户向量模做过滤
    val cosSimilarity = vecMatrixCosineSimilarity(sc, userFeatures, bUserFeatures)
    cosSimilarity.cache()
    cosSimilarity.map(x => Seq(x._1, x._2, x._3).mkString("~"))
      .saveAsTextFile("/user/iflyrd/work/jjhu/kuyin/sim")

    val topKSimilarityUser = getSimilarTopKUser(
      simScoreThreshold, topK, cosSimilarity)
    saveInHbase(sc, config, hbaseHost, similarityHbaseTable
      , similarityHbaseCol, topKSimilarityUser)

    sc.stop()
  }

  /**
    * 从HDFS读入用户特征矩阵
    * String Vector
    *
    * @param sc
    * @param userFeaturesInput
    * @param parallelism
    * @return
    */
  def loadUserFeaturesFromHdfs(sc: SparkContext,
                               userFeaturesInput: String,
                               parallelism: Int)
  : RDD[(String, Vector, Double)] = {
    sc.objectFile[(String, Vector)](userFeaturesInput, 1000)
      .map {
        case (uid, vec) => (uid, vec, Vectors.norm(vec, 2.0))
      }
      .filter {
        case (uid, vec, norm) => norm > 0d
      }
  }

  /**
    * topK相似度用户存入Hbase
    *
    * @param sc
    * @param conf
    * @param hbaseHost
    * @param hbaseTable
    * @param hbaseColumn
    * @param simlilarRdd
    */
  def saveInHbase(sc: SparkContext,
                  conf: Config,
                  hbaseHost: String,
                  hbaseTable: String,
                  hbaseColumn: String,
                  simlilarRdd: RDD[(String, String)]): Unit = {

    val resultRdd = simlilarRdd.map {
      case (uid, simUids) => {
        val resMap: Map[String, String] = Map(hbaseColumn -> simUids)
        (uid, resMap)
      }
    }

    HBaseUtil.setHBaseConf(hbaseHost) //还是加上比较好,完整性
    HBaseUtil.hbaseWrite(hbaseTable,
      resultRdd
    )
  }

  /**
    * 取最相似的k个用户
    *
    * @param similarityScoreThreshold
    * @param similarTopK
    * @param userSimilarity
    * @return (uid, similarUid1~similarUid2)
    */
  def getSimilarTopKUser(similarityScoreThreshold: Double,
                         similarTopK: Int,
                         userSimilarity: RDD[(String, String, Double)])
  : RDD[(String, String)] = {
    require(similarTopK > 0, "Please check your input, need 'topK > 0'")

    println("similarityScoreThreshold = " + similarityScoreThreshold)

    val result = userSimilarity.filter {
      case (uid1, uid2, sim) => sim > similarityScoreThreshold //需要看分布
    }
      .map {
        case (uid1, uid2, sim) => (uid1, (uid2, sim))
      }
      .mapPartitions {
        iter => {//每个分片内先算出topK的值
          val partArr = iter.toArray
          println("partition size = " + partArr.size)
          val tmp = partArr.groupBy(_._1)
            .map {
              case (uid, arr) => {
                val arr1 = arr.map {
                  case (uid1, (uid2, score)) => (uid2, score)
                }
                (uid, arr1)
              }
            }
            .map {
              case (uid, arr) => {
                val sort = heapSort(arr, 0, similarTopK)
                (uid, sort)
              }
            }.toIterator
          tmp
        }
      }
      .reduceByKey{
        case (x, y) => {
          val both = x ++ y
          heapSort(both, 0, similarTopK)
        }
      }
      .map {
        case (uid1, arr) => (uid1, arr.map {
          case (uid, sim) => Seq(uid, CommonService.formatDouble(sim, 3)).mkString(":")
        })
      }
      .map {
        case (uid, uidArr) => (uid, uidArr.mkString("~"))
      }
    result
  }

  /**
    * 小顶堆获取TopK
    *
    * @param simArr
    * @param root
    * @param len
    * @return
    */
  def heapSort(simArr: Array[(String, Double)],
               root: Int,
               len: Int)
  : Array[(String, Double)] = {
    if (simArr.length <= len) {
      return simArr
    }

    println("HeapSort input size = " + simArr.length)

    val minHeap: Array[(String, Double)] = new Array[(String, Double)](len)
    for (i <- 0 until len) {
      minHeap(i) = ("0", Double.MinValue)
    }

    for (item <- simArr) {
      if (item._2 > minHeap(0)._2) {
        minHeap(0) = item
        adjustHeap(minHeap, 0, len)
      }
    }

    minHeap
  }

  /**
    * 每次小顶堆调整
    *
    * @param heap
    * @param root
    * @param len
    */
  def adjustHeap(heap: Array[(String, Double)],
                 root: Int,
                 len: Int): Unit = {
    val left = 2 * root + 1 //左孩子下标
    val right = 2 * root + 2 //右孩子下标
    var min = root

    if (left < len && heap(left)._2 < heap(min)._2) {
      min = left
    }

    if (right < len && heap(right)._2 < heap(min)._2) {
      min = right
    }

    if (min != root) {
      swap(heap, root, min)
      adjustHeap(heap, min, len)
    }
  }

  /**
    * 交换堆中元素位置
    *
    * @param heap
    * @param positionX
    * @param positionY
    */
  def swap(heap: Array[(String, Double)],
           positionX: Int,
           positionY: Int): Unit = {
    val tmp = heap(positionX)
    heap(positionX) = heap(positionY)
    heap(positionY) = tmp
  }

  /**
    * 计算矩阵中向量与向量之间的余弦相似度
    *
    * @param sc
    * @param matrix
    * @param bMatrix
    * @return
    */
  def vecMatrixCosineSimilarity(sc: SparkContext,
                                matrix: RDD[(String, Vector, Double)],
                                bMatrix: Broadcast[Array[(String, Vector, Double)]])
  : RDD[(String, String, Double)] = {
    matrix.flatMap {
      case (uid1, vec1, norm1) => {
        val cMatrix = bMatrix.value.filter(_._1 != uid1) //_._1 为uid
        cMatrix.map {
          case (uid2, vec2, norm2) => {
            val dotV = dot(vec1, vec2)
            (uid2, vec2, norm2, dotV)
          }
        }
          .filter(_._4 > 0.0d) //用户特征向量很稀疏, 很多点积为0, 可提前过滤
          .map {
          case (uid2, vec2, norm2, dotV) => {
            val len = norm2 * norm1
            val sim = dotV / len
            (uid1, uid2, sim)
          }
        }
      }
    }
      .filter(_._3 > 0.0d)
  }

  /**
    * 有输出格式的除法
    *
    * @param dividend
    * @param divisor
    * @param decimalBit
    * @return
    */
  def divisionWithFormatResult(dividend: Double,
                               divisor: Double,
                               decimalBit: Int): String = {
    val nFormat = NumberFormat.getNumberInstance()
    var result = "0"
    nFormat.setMaximumFractionDigits(decimalBit)

    if (divisor > 0) {
      result = nFormat.format(((dividend * 1.0) / divisor))
    }
    result
  }

  /**
    * Belong to the input type, use different way to calculate got.
    *
    * @param x Vector
    * @param y Vector
    * @return
    */
  def dot(x: Vector, y: Vector): Double = {
    require(x.size == y.size,
      "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
        " x.size = " + x.size + ", y.size = " + y.size)
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) =>
        dot(dx, dy)
      case (sx: SparseVector, dy: DenseVector) =>
        dot(sx, dy)
      case (dx: DenseVector, sy: SparseVector) =>
        dot(sy, dx)
      case (sx: SparseVector, sy: SparseVector) =>
        dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }

  /**
    * Return NetlibBLAS object, if object not exists, new and return it.
    *
    * @return
    */
  private def f2jBLAS: NetlibBLAS = {
    if (_f2jBLAS == null) {
      _f2jBLAS = new F2jBLAS
    }
    _f2jBLAS
  }

  /**
    * 向量点积 A.B = (a1,a2,a3).(b1,b2,b3) = a1*b1 + a2*b2 + a3*b3
    *
    * @param x DenseVector
    * @param y DenseVector
    * @return
    */
  private def dot(x: DenseVector, y: DenseVector): Double = {
    val n = x.size
    f2jBLAS.ddot(n, x.values, 1, y.values, 1)
  }

  /**
    * 向量点积 A.B = (a1,a2,a3).(b1,b2,b3) = a1*b1 + a2*b2 + a3*b3
    *
    * @param x SparseVector
    * @param y DenseVector
    * @return
    */
  private def dot(x: SparseVector, y: DenseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.length

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  /**
    * 向量点积 A.B = (a1,a2,a3).(b1,b2,b3) = a1*b1 + a2*b2 + a3*b3
    *
    * @param x SparseVector
    * @param y SparseVector
    * @return
    */
  private def dot(x: SparseVector, y: SparseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices //值不为0的下标
    val yValues = y.values
    val yIndices = y.indices
    val xSize = xIndices.size

    val yIndiceValueMap = yIndices.zip(yValues).toMap

    var i = 0
    var sum = 0.0
    // y catching x
    while (i < xSize) {
      val xIndice = xIndices(i)
      if (yIndiceValueMap.contains(xIndice)) {
        sum += xValues(i) * yIndiceValueMap(xIndice)
      }
      i += 1
    }
    sum
  }


  /**
    * 初始化
    *
    * @param conf
    */
  def initParam(conf: Config): Unit = {
    hbaseHost = conf.getString("hbase.host")
    similarityHbaseTable = conf.getString("hbase.simTable")
    similarityHbaseCol = conf.getString("hbase.simCol")
    simScoreThreshold = conf.getString("simScoreThreshold").toDouble
    topK = conf.getString("topK").toInt
    parallelism = conf.getInt("parallelism")
    checkPointDir = conf.getString("checkPointDir")
  }

  @transient private var _f2jBLAS            : NetlibBLAS = _
  private            var hbaseHost           : String     = _
  private            var similarityHbaseTable: String     = _
  private            var similarityHbaseCol  : String     = _
  private            var simScoreThreshold   : Double     = _
  private            var topK                : Int        = _
  private            var parallelism         : Int        = _
  private            var checkPointDir       : String     = _
}
