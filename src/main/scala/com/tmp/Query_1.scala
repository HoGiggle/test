package com.tmp

import com.hankcs.hanlp.tokenizer.NLPTokenizer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/12/21.
  */
object Query_1 {
    def main(args: Array[String]): Unit = {
        val Array(useDate) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        //path setting
        val inPath = s"/user/iflyms/inter/QueryInfo/$useDate/"
        val stopPath = "/user/iflyrd/falco/profile/ItemProfile/Resource/StopWords"
        val outPath = s"/user/iflyrd/work/jjhu/sex/query/segment/$useDate"

        //data loading & broadcast
        val stopWords = sc.broadcast(sc.textFile(stopPath).distinct().collect())
        val throwNature = sc.broadcast(Array("w", "e", "m", "p"))
        val queryData = sc.textFile(inPath).map(_.split("~"))
            .filter(_.length >= 5)
            .filter{
                case Array(uid, imei, query, pkg, date) => usableFilter(uid, imei, query)
            }
            .map{
                case Array(uid, imei, query, pkg, date) =>
                    Seq(uid, imei, segment(query, stopWords.value, throwNature.value)).mkString("~")
            }


        //save
        queryData.coalesce(64).saveAsTextFile(outPath)
        sc.stop()
    }

    def usableFilter(uid:String,
                     imei:String,
                     query:String):Boolean = {
        ((query.length > 0) && (query.length < 20)) &&
            (uid.length >= 15) &&
            (imei.length == 15)
    }

    def segResultFilter(item:(String, String),
                        stopWords:Array[String],
                        throwNature:Array[String]):Boolean= {
        val basicF = (!throwNature.contains(item._2)) && (!stopWords.contains(item._1))
        val smartR = if (item._2 == "a") item._1.length > 1 else true
        basicF && smartR
    }

    def segment(query:String,
                stopWords:Array[String],
                throwNature:Array[String]):String = {
        import collection.JavaConverters._
        val segList = NLPTokenizer.segment(query).asScala
        val words = segList.map{
            seg => {
                (seg.word, seg.nature.toString)
            }
        }
            .filter(x => segResultFilter(x, stopWords, throwNature))
            .map{
                case (word, nature) => Seq(word, nature).mkString(":")
            }
        words.mkString(",")
    }
}
