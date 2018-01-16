package com.kuyin.structure

/**
  * Created by jjhu on 2017/2/27.
  */
object KYLSLogField {
    // ext字段及衍生字段
    val EXT: String = "ext"
    val EXT_RingName: String = "ringname"
    val EXT_SingerName: String = "singername"
    val EXT_LisDur: String = "lisdur" //试听时长 /ms

    // 系统服务相关字段
    val SYS_ReqName: String = "reqname"
    val SYS_Ret: String = "ret"
    val SYS_RetDesc: String = "retdesc"
    val SYS_ResObj: String = "resobj"
    val SYS_Dur: String = "dur"

    // 空值常量
    val STRING_VALUE_DEFAULT: String = ""
    val INT_VALUE_DEFAULT: String = "0"

    //主要判断字段
    val TYPE: String = "type"
    val ObjType: String = "objtype"
    val Obj: String = "obj"               //itemId
    val Evt: String = "evt"
    val CUid: String = "cuid"             //uid
    val OSId: String = "osid"
    val TP: String = "tp"
}
