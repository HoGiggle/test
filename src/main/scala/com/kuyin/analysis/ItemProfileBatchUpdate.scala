package com.kuyin.analysis

import com.kuyin.structure.RecommItemField
import com.util.{ETLUtil, HBaseUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats

/**
  * Created by jjhu on 2017/3/1.
  */
object ItemProfileBatchUpdate {
    def main(args: Array[String]): Unit = {
        val Array(date) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        //path setting
        val itemPath = s"/user/iflyrd/kuyin/recommend/App/process/profile/item/$date/"
        val indexPath = "/user/iflyrd/kuyin/inter/itemTagsIndex/*/"

        //day ring info load
        val dayRdd = sc.textFile(itemPath)
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
                    (ringId, (music.ringName, music.singer, tagArr, music.toSet, music.toTry))
                }
            }
            .cache()

        //encode new tag with index
        val oldTagIndexRdd = sc.textFile(indexPath)
            .map(line => {
                val Array(tag, index) = line.split("~")
                (tag, index)
            })
        val bOldTagIndex = sc.broadcast(oldTagIndexRdd.collect().toMap)

        val tagsLocal = dayRdd.flatMap{
            case (ringId, (ringName, singer, tagArr, toSet, toTry)) => {
                tagArr
            }
        }
            .filter(!bOldTagIndex.value.contains(_))
            .distinct()
            .collect()

        val oldTagSize = bOldTagIndex.value.size
        val newTagsIndex = for (i <- tagsLocal.indices) yield (tagsLocal(i), oldTagSize + i + 1) //svmlib feature index 开始值为1
        val bNewTagsIndex = sc.broadcast(newTagsIndex.toMap)  // broadcast new tag and index

        //batch get old item profile
        val allRowKeys = dayRdd.map(_._1)
        val oldItemsRdd = HBaseUtil.batchGetFromHbase(allRowKeys, 128, "iflyrd:KylsItemProfile")
        val hbaseRdd = dayRdd.leftOuterJoin(oldItemsRdd)
            .map{
                case (ringId, ((ringName, singer, tagArr, toSet, toTry), opt)) => {
                    if (opt.isEmpty){ //new item, update all information
                        val tagFeature = tagArr.map(tag => {
                            var tagIndex: String = null
                            if (bOldTagIndex.value.contains(tag)){
                                tagIndex = bOldTagIndex.value(tag)
                            } else {
                                tagIndex = bNewTagsIndex.value(tag).toString
                            }
                            Seq(tagIndex, "1.0").mkString(":") //1.0 as init value
                        }).mkString(" ")
                        val mapValue = Map(
                            s"cf:${RecommItemField.RingName}" -> ringName,
                            s"cf:${RecommItemField.Singer}" -> singer,
                            s"cf:${RecommItemField.Tags}" -> tagFeature,
                            s"cf:${RecommItemField.ToSet}" -> toSet.toString,
                            s"cf:${RecommItemField.ToTry}" -> toTry.toString
                        )
                        (ringId, mapValue)
                    } else { //old item, just update 'toSet' and 'toTry' field
                        val mapValue = Map(
                            s"cf:${RecommItemField.ToSet}" -> toSet.toString,
                            s"cf:${RecommItemField.ToTry}" -> toTry.toString
                        )
                        (ringId, mapValue)
                    }
                }
            }

        //new tag and index save in hdfs, and update item info to hbase.
        sc.parallelize(newTagsIndex).map{
            case (tag, index) => Seq(tag, index).mkString("~")
        }.coalesce(1).saveAsTextFile(s"/user/iflyrd/kuyin/inter/itemTagsIndex/$date")

        HBaseUtil.hbaseWrite("iflyrd:KylsItemProfile", hbaseRdd)

        sc.stop()
    }
}
