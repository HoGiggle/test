package com.expriment

import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.util.{AsciiUtil, CommonService}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

/**
  * Created by jjhu on 2018/1/11.
  */
object Word2VecTest {
  def main(args: Array[String]): Unit = {
    val sc = CommonService.scClusterInit(this.getClass.getName)
    if (args(0) == "1") segment(sc)
    if (args(1) == "1") word2VecRun(sc)

    //find similar words
    if (args(0) == "2") {
      val model = Word2VecModel.load(sc, "/user/iflyrd/work/jjhu/ml/word2vec/model")
      val like = model.findSynonyms(args(1), 40)
      for ((item, cos) <- like) {
        println(s"$item  $cos")
      }
    }
    sc.stop()
  }

  def localSegmentTest() = {
    val sc = CommonService.scLocalInit(this.getClass.getName)
    val stopWordPath = "file:///E:\\data\\mllib\\stopWords"
    val bcStopWords = sc.broadcast(sc.textFile(stopWordPath).collect().toSet)

    val data = sc.textFile("file:///E:\\data\\mllib\\news_train_data")
      .map(AsciiUtil.sbc2dbcCase)
      .mapPartitions(it =>{
        val tmp = it.toArray.map(ct => {
          try {
            val nlpList = NLPTokenizer.segment(ct)
            import scala.collection.JavaConverters._
            nlpList.asScala.map(term => term.word).filter(!bcStopWords.value.contains(_)).mkString(" ")
          } catch {
            case e: Exception => println(ct);""
          }
        })
          .toIterator
        tmp
      })
    for (content <- data.take(1)) println(content)
    sc.stop()
  }

  def word2VecRun(sc:SparkContext) = {
    val input = sc.textFile("/user/iflyrd/work/jjhu/ml/word2vec/segment").map(line => line.split(" ").toSeq)
    //model train
    val word2vec = new Word2Vec()
      .setVectorSize(50)
      .setNumPartitions(64)

    val model = word2vec.fit(input)
    println("model word size: " + model.getVectors.size)

    //Save and load model
    model.save(sc, "/user/iflyrd/work/jjhu/ml/word2vec/model")
    val local = model.getVectors.map{
      case (word, vector) => Seq(word, vector.mkString(" ")).mkString(":")
    }.toArray
    sc.parallelize(local).saveAsTextFile("/user/iflyrd/work/jjhu/ml/word2vec/vectors")

    //val sameModel = Word2VecModel.load(sc, "/user/iflyrd/work/jjhu/ml/word2vec/model")
  }

  def segment(sc:SparkContext): Unit = {
    //stop words
    val stopWordPath = "/user/iflyrd/work/jjhu/ml/stopwords/*"
    val bcStopWords = sc.broadcast(sc.textFile(stopWordPath).collect().toSet)

    //content segment
//    val inPath = "/user/iflyrd/work/jjhu/ml/word2vec/data/*"
    val inPath = "/user/iflyms/inter/QueryInfo/20180226/*"
    val segmentRes = sc.textFile(inPath)
      .map(_.split("~", -1))
      .filter(_.length == 5)
      .map(x => x(2))
      .map(AsciiUtil.sbc2dbcCase)
      .mapPartitions(it =>{
        it.map(ct => {
          try {
            val nlpList = NLPTokenizer.segment(ct)
            import scala.collection.JavaConverters._
            nlpList.asScala.map(term => term.word)
              .filter(!bcStopWords.value.contains(_))
              .mkString(" ")
          } catch {
            case e: NullPointerException => println(ct);""
          }
        })
      })

    //save
    segmentRes.saveAsTextFile("/user/iflyrd/work/jjhu/ml/word2vec/segment")
    bcStopWords.unpersist()
  }
}
