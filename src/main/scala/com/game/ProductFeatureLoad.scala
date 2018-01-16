package com.game

import java.text.SimpleDateFormat

import com.util.{CommonService, HBaseUtil, JDHbaseColumn}

/**
  * Created by jjhu on 2017/4/5.
  */
object ProductFeatureLoad {
  def main(args: Array[String]): Unit = {
    // Spark log level & sc init
    val sc = CommonService.scClusterInit(this.getClass.getName)

    //商品数据
    val productPath = "/user/iflyrd/work/jjhu/ccf_jd_v1/product/product_raw"
    val productData = sc.textFile(productPath)
      .map(_.split("~", -1))
      .filter(_.length == 6)
      .map{
        case Array(itemId, attr1, attr2, attr3, cate, brand) =>
          (itemId.toInt, (attr1, attr2, attr3))  //先保留cate、brand信息, 防止行为数据中信息缺失
      }
    val bProduct = sc.broadcast(productData.collect().toMap)
    println("product num = " + bProduct.value.size)

    //评论数据聚合
    val commentPath = "/user/iflyrd/work/jjhu/ccf_jd_v1/comment/comment_raw"
    val commentData = sc.textFile(commentPath)
      .map(_.split("~", -1))
      .filter(_.length == 5)
      .map{
        case Array(date, itemId, commentNum, isBad, badRate) => (itemId, (date, commentNum, isBad, badRate))
      }
      .reduceByKey{
        case (x, y) => if (x._1 > y._1) x else y  //取最近的评论信息
      }
      .map{
        case (itemId, (date, commentNum, isBad, badRate)) => (itemId.toInt, (commentNum, isBad, badRate))
      }
    val bComment = sc.broadcast(commentData.collect().toMap)
    println("comment num = " + bComment.value.size)

    //行为数据分析
    val actionPath = "/user/iflyrd/work/jjhu/ccf_jd_v1/action/*"
    val parseDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val minuteDf = new SimpleDateFormat("MMddHHmm") //一分钟之内的行为只算一次
    val DEFAULT_DATE = "01010000"

    //todo 一定时间范围内的行为, 参数添加
    val actionData = sc.textFile(actionPath)
      .map(_.split("~", -1))
      .filter(_.length == 7)
      .map{
        case Array(uid, itemId, time, modelId, actType, cate, brand) => {
          val modelIdInt = if (modelId.length > 0) modelId.toInt else -999  //modelId 默认int = -999
          var minTime = DEFAULT_DATE
          try {
            minTime = minuteDf.format(parseDf.parse(time))
          } catch {
            case e: Exception => {
              e.printStackTrace()
            }
          }

          (uid.toInt, itemId.toInt, minTime.toInt, modelIdInt, actType.toInt, cate.toInt, brand.toInt)
        }
      }
      .distinct()//过滤一分钟内的多次相同行为
      .map{
        case (uid, itemId, time, modelId, actType, cate, brand) => {
          actType match {//不同行为映射
            case 1 => ((itemId, cate, brand), (1, 0, 0, 0, 0, 0))
            case 2 => ((itemId, cate, brand), (0, 1, 0, 0, 0, 0))
            case 3 => ((itemId, cate, brand), (0, 0, 1, 0, 0, 0))
            case 4 => ((itemId, cate, brand), (0, 0, 0, 1, 0, 0))
            case 5 => ((itemId, cate, brand), (0, 0, 0, 0, 1, 0))
            case 6 => ((itemId, cate, brand), (0, 0, 0, 0, 0, 1))
            case _ => ((itemId, cate, brand), (0, 0, 0, 0, 0, 0))
          }
        }
      }
      .reduceByKey{
        case (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6)
      }
      .map{
        case ((itemId, cate, brand), (browse, addCart, removeCart, buy, focus, click)) => {
          //不同行为的下单率
          val brwRate = CommonService.formatDiv(buy, browse, 3)
          val addCartRate = CommonService.formatDiv(buy, addCart, 3)
          val focusRate = CommonService.formatDiv(buy, focus, 3)
          val clickRate = CommonService.formatDiv(buy, click, 3)

          (itemId, (cate, brand,
            browse, addCart, removeCart, buy, focus, click,
            brwRate, addCartRate, focusRate, clickRate))
        }
      }

    val bJDHBaseCol = sc.broadcast(JDHbaseColumn)
    val pJoinData = actionData.map{
      case (itemId, (cate, brand, browse, addCart, removeCart, buy, focus, click, brwRate, addCartRate, focusRate, clickRate)) => {
        var resMap = Map(bJDHBaseCol.value.P_Cate -> cate.toString,
                         bJDHBaseCol.value.P_Brand -> brand.toString,
                         bJDHBaseCol.value.P_Browse -> browse.toString,
                         bJDHBaseCol.value.P_Add_Cart -> addCart.toString,
                         bJDHBaseCol.value.P_Remove_Cart -> removeCart.toString,
                         bJDHBaseCol.value.P_Buy -> buy.toString,
                         bJDHBaseCol.value.P_Focus -> focus.toString,
                         bJDHBaseCol.value.P_Click -> click.toString,
                         bJDHBaseCol.value.P_Browse_Rate -> brwRate,
                         bJDHBaseCol.value.P_AddCart_Rate -> addCartRate,
                         bJDHBaseCol.value.P_Focus_Rate -> focusRate,
                         bJDHBaseCol.value.P_Click_Rate -> clickRate)
        //商品属性枚举特征
        if (bProduct.value.contains(itemId)){
          val attr = bProduct.value(itemId)
          val pMap = Map(bJDHBaseCol.value.P_Attr1 -> attr._1,
            bJDHBaseCol.value.P_Attr2 -> attr._2,
            bJDHBaseCol.value.P_Attr3 -> attr._3
          )
          resMap = resMap ++ pMap
        }

        //商品评论特征
        if (bComment.value.contains(itemId)){
          val comment = bComment.value(itemId)
          val cMap = Map(bJDHBaseCol.value.P_Comment -> comment._1,
            bJDHBaseCol.value.P_Had_Bad -> comment._2,
            bJDHBaseCol.value.P_Bad_Rate -> comment._3
          )
          resMap = resMap ++ cMap
        }

        (itemId.toString, resMap)
      }
    }

    HBaseUtil.hbaseWrite("iflyrd:CCF_Product", pJoinData)
    sc.stop()
  }
}
