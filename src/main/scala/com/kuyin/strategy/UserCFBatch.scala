package com.kuyin.strategy

import com.kuyin.structure.{BehaviorLevel, RecommendALG}
import com.util.{HBaseUtil, HdfsConfHelper, LocalConfHelper}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/3/9.
  */
object UserCFBatch extends RecommendUtils{
  def main(args: Array[String]): Unit = {
    val Array(confPath, inputRange, runDate, mode) = args
    val config = if (mode == "cluster") {
      HdfsConfHelper.getConfig(confPath).getConfig("job")
    } else {
      LocalConfHelper.getConfig(confPath).getConfig("job")
    }
    PropertyConfigurator.configure(config.getString("log4j"))
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getName + s"_$runDate"))


    /** 用户过去三天的试听铃声 join 推荐库铃声 */
    //推荐库铃声
    val validItemRDD = HBaseUtil.hbaseRead(sc,
      config.getString("hbase.itemTable"), config.getString("hbase.itemColumn"))
      .cache()
    val bValidItemIdSet = sc.broadcast(validItemRDD.map(x => x._1).collect().toSet)

    //试听铃声
    val audiPath = Seq(config.getString("hdfs.audiInput"), inputRange).mkString("/")
    val audiData = sc.textFile(audiPath).map(_.split("~"))
      .filter(x => (x.length >= 5) && (x(4).toInt == BehaviorLevel.Audition) &&
        (x(3).toInt == BehaviorLevel.UserSelf) && //主动试听行为
        bValidItemIdSet.value.contains(x(2)))     //推荐库的铃声过滤
      .map{
        case Array(date, uid, itemId, autoFlag, score) => (uid, Set(itemId))
      }
      .reduceByKey{
        case (x, y) => x ++ y
      }

    /** 用户协同 */
    //需要个性化用户 根据用户行为, 昨天满足灰度条件的用户
    val dayGrayUserPath = Seq(config.getString("hdfs.grayPath"), runDate).mkString("/")
    val grayUser = sc.textFile(dayGrayUserPath).map(_.split("~"))
      .filter(_.length == 2)
      .map{
        case Array(date, uid) => uid
      }

    //读取topK相似用户, 协同铃声
    val topKSimCol = sc.broadcast(config.getString("hbase.topKSimCol"))
    val cfResultRDD = HBaseUtil.batchGetFromHbase(grayUser,
      config.getInt("hbase.topKSimPartition"), config.getString("hbase.topKSimTable"))
      .flatMap{
        case (uid, map) => {
          val simValue = Bytes.toString(map.getOrElse(topKSimCol.value, Array[Byte]()))
          val simUserList = simValue.split("~", -1)
            .map(item => item.split(":", -1)(0))
          simUserList.map(simUid => (simUid, uid))
        }
      }
      .join(audiData)
      .map{
        case (simUid, (uid, itemSet)) => (uid, itemSet)
      }
      .reduceByKey{
        case (x, y) => x ++ y
      }
      .cache()
    //todo 如果协同效果不好, 可以考虑计算协同的item与user之间的相似性, 再过滤

    cfResultRDD.map{
      case (uid, itemSet) => (itemSet.size, 1)
    }

      .reduceByKey(_ + _)
      .sortByKey()
      .map{
        case (cnt, userCnt) => Seq(cnt, userCnt).mkString("~")
      }
      .coalesce(1)
      .saveAsTextFile("/user/iflyrd/work/jjhu/kuyin/tmp/userCfRange")


    /** save userCf result in hdfs and hbase */
    val userCfRes = cfResultRDD.flatMap{
      case (uid, itemSet) => itemSet.map{
        itemId => (uid, itemId)
      }
    }.cache()
    saveRecommHDFS(userCfRes, config.getString("hdfs.result"),
      runDate, config.getInt("hdfs.resPartition"), RecommendALG.UserCF.toString)
    saveRecommHBASE(sc, config.getString("hbase.recomTable"),
      config.getString("hbase.zkQuorum"), config.getString("hbase.recomCol"),
      userCfRes, runDate, RecommendALG.UserCF)


    //finally
    validItemRDD.unpersist()
    cfResultRDD.unpersist()
    userCfRes.unpersist()
    sc.stop()
  }
}
