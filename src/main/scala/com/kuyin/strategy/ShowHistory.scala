package com.kuyin.strategy

import com.kuyin.structure.KYLSLogField
import com.util.{CommonService, ETLUtil, HBaseUtil}

/**
  * 猜你喜欢的展示历史
  * Created by jjhu on 2017/4/12.
  */
object ShowHistory {
  def main(args: Array[String]): Unit = {
    val Array(runDate) = args
    val sc = CommonService.scClusterInit(this.getClass.getName)

    //path
    val path = s"/user/iflyrd/kuyin/input/type_80000/$runDate"

    //猜你喜欢的展示情况
    val data = ETLUtil.readMap(sc, path)
      .map{
        m => (m.getOrElse(KYLSLogField.CUid, ""),
          m.getOrElse(KYLSLogField.Obj, ""),
          m.getOrElse(KYLSLogField.OSId, ""),
          m.getOrElse(KYLSLogField.ObjType, ""),
          m.getOrElse(KYLSLogField.Evt, ""))
      }
      .filter{
        case (uid, itemId, osid, objType, evt) =>
          (osid.toLowerCase == "android") && (objType == "10") && (evt == "33") && (uid.length > 0) && (itemId.length > 0)
      }
      .map{
        case (uid, itemId, osid, objType, evt) => (uid, itemId)
      }
      .distinct()  //todo distinct具体实现, reduceByKey手动控制会不会效率更高？

    val hbaseData = data.map{
      case (uid, itemId) => (Seq(uid, itemId).mkString("~"), Map("cf:date" -> runDate)) //目前来看只需要Set, 存日期方便查问题
    }

    //save hbase & stop
    HBaseUtil.hbaseWrite("iflyrd:KylsShowHistory", hbaseData)
    sc.stop()
  }
}
