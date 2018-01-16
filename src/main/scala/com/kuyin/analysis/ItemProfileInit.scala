package com.kuyin.analysis

import com.kuyin.structure.RecommItemField
import com.util.{ETLUtil, HBaseUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats

object ItemProfileInit {
    def main(args: Array[String]): Unit = {
        val Array(date) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        //path
        val path = s"/user/iflyrd/kuyin/recommend/App/process/profile/item/$date/"

        //main
        val rdd = sc.textFile(path)
            .map(line => {
                val jValue = ETLUtil.json4sParse(line)
                implicit val formats = DefaultFormats
                val music = jValue.extract[Music]
                (music.ringId, music)
            })
            .filter{
                case (ringId, music) =>
                    (ringId.length > 0) && (music.ringName.length > 0) && (music.singer.length > 0) //入库完整性过滤
            }
            .reduceByKey{
                case (x, y) => if (x.toSet >= y.toSet) x else y //多天重复
            }
            .map{
                case (ringId, music) => {
                    val tagArr = music.tags.trim.split("\\|")
                    (ringId, music.ringName, music.singer, tagArr, music.toSet, music.toTry)
                }
            }
            .cache()

        // tags collect
        val tagsLocal = rdd.flatMap{
            case (ringId, ringName, singer, tagArr, toSet, toTry) => {
                tagArr
            }
        }
            .distinct()
            .collect()

        // broadcast tags with index
        val tagsIndex = for (i <- tagsLocal.indices) yield (tagsLocal(i), i + 1) //svmlib feature index 开始值为1
        val bTagsIndex = sc.broadcast(tagsIndex.toMap)

        // save tags index
        sc.parallelize(tagsIndex).map{
            case (tag, index) => Seq(tag, index).mkString("~")
        }.coalesce(1).saveAsTextFile("/user/iflyrd/kuyin/inter/itemTagsIndex/20170301")

        // save item info to hbase
        val hbaseRdd = rdd.map{
            case (ringId, ringName, singer, tagArr, toSet, toTry) => {
                val tagFeature = tagArr.map(tag => Seq(bTagsIndex.value(tag), "1.0").mkString(":")).mkString(" ")
                val mapValue = Map(
                    s"cf:${RecommItemField.RingName}" -> ringName,
                    s"cf:${RecommItemField.Singer}" -> singer,
                    s"cf:${RecommItemField.Tags}" -> tagFeature,
                    s"cf:${RecommItemField.ToSet}" -> toSet.toString,
                    s"cf:${RecommItemField.ToTry}" -> toTry.toString
                )
                (ringId, mapValue)
            }
        }
        HBaseUtil.hbaseWrite("iflyrd:KylsItemProfile", hbaseRdd)

        sc.stop()
    }
}
