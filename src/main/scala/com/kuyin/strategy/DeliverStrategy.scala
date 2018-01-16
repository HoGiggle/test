package com.kuyin.strategy

import com.util.{CommonService, HBaseUtil, HdfsConfHelper, LocalConfHelper}
import org.apache.hadoop.hbase.util.Bytes

/**
  * 投递策略
  * Created by jjhu on 2017/4/12.
  */

case class CtrItem(itemId: String){
  var isVisited: Boolean = false
  var isUsed: Boolean = false
  def this(itemId: String, visited: Boolean, used: Boolean) = {
    this(itemId)
    this.isVisited = visited
    this.isUsed = used
  }

//  def getItemId = itemId
}

object DeliverStrategy {
  def main(args: Array[String]): Unit = {
    val Array(confPath, runDate, mode) = args
    val config = if (mode == "cluster") {
      HdfsConfHelper.getConfig(confPath).getConfig("job")
    } else {
      LocalConfHelper.getConfig(confPath).getConfig("job")
    }

    //sc init & broadcast
    val sc = CommonService.scClusterInit(this.getClass.getName)
    val bDefaultStep = sc.broadcast(config.getInt("delivery.step"))
    val bOnceSize = sc.broadcast(config.getInt("delivery.onceSize"))


    //scan 历史库, 优化:rowRange filter  todo 查询优化
    val history = HBaseUtil.hbaseRead(sc, "iflyrd:KylsShowHistory")
      .map{
        case (key, value) => {
          val Array(uid, itemId) = key.split("~", -1)
          ((uid, itemId), 1)
        }
      }

    //ctr 打分结果
    val ctrPath = ""
    val ctrRes = sc.textFile(ctrPath).map(_.split("~"))
      .filter(_.length == 3)
      .map{
        case Array(uid, itemId, score) => ((uid, itemId), score.toDouble)
      }

    //batch get itemId 推荐库获取歌手信息
    val singer = HBaseUtil.batchGetFromHbase(ctrRes.map(_._1._2).distinct(), 100, "iflyrd:KylsItemProfile")
      .map{
        case (itemId, m) => {
          val singer = Bytes.toString(m.getOrElse("cf:singer", Array[Byte]()))
          (itemId, singer)
        }
      }
    val bSinger = sc.broadcast(singer.collect().toMap)
    bSinger


    val deliverResult = ctrRes.leftOuterJoin(history)
      .filter{
        case ((uid, itemId), (score, opt)) => opt.isEmpty
      }
      .map{
        case ((uid, itemId), (score, opt)) => (uid, (itemId, score))
      }
      .groupByKey()
      .map{
        case (uid, iter) => {
          val ctrSortArr = iter.toArray.sortBy(-_._2).map(_._1)
          val deliverSortArr = singleSinger(ctrSortArr, bSinger.value, bDefaultStep.value, bOnceSize.value)
          (uid, deliverSortArr)
        }
      }

    //投递结果暂存redis

    ctrRes
  }

  /**
    * 投递策略
    * @param data 每位用户经过ctr排序后的召回结果
    * @param singer 推荐库中歌曲、歌手映射表, Map[itemId, singerName]
    * @param defaultStep 默认步长, 可配置
    * @param onceSize 一次推荐的铃声数, 客户端目前一屏显示5首
    * @return 投递顺序
    */
  def singleSinger(data: Array[String],
                   singer: Map[String, String],
                   defaultStep: Int,
                   onceSize: Int)
                   : Array[String] = {
    if ((data == null) || (data.length <= onceSize)){
      return data
    }

    val objData: Array[CtrItem] = new Array[CtrItem](data.length)
    for (i <- data.indices){
      objData(i) = CtrItem(data(i))
    }

    //步长确定
    val realStep = data.length / onceSize
    val step = if (realStep < defaultStep) realStep else defaultStep

    //
    val singerSet = new scala.collection.mutable.HashSet[String]()
    var len = 0
    var nextLocation = 0
    var i = 0
    val recList = new scala.collection.mutable.MutableList[String]()
    while (len < onceSize){
      if (objData(i).isVisited) {
        if (objData(i).isUsed){
          i += 1
        } else {
          //多样性不能满足
          i = nextLocation
          while (len < onceSize){
            recList += objData(i).itemId
            len += 1
            i = (i + step) % data.length
          }
        }
      } else {
        objData(i).isVisited = true
        if (!singerSet.contains(singer(objData(i).itemId))){ //不冲突 定义冲突模块 可以抽象成单独的rule
          singerSet += singer(objData(i).itemId)//todo 推荐库更新问题需要考虑会不会抛异常
          recList += objData(i).itemId
          objData(i).isUsed = true
          len += 1
          //next location
          i = (i + step) % data.length
          nextLocation = i
        } else { //冲突
          i += 1
        }
      }
    }

    //
    val remainList = scala.collection.mutable.MutableList[String]()
    for (ctrItem <- objData) {
      if (!ctrItem.isUsed){
        remainList += ctrItem.itemId
      }
    }
    recList.toArray ++ singleSinger(remainList.toArray, singer, defaultStep, onceSize)
  }
}
