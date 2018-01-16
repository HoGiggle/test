package com.cheat

import com.protobf.InputUser
import com.util.{CommonService, HBaseUtil, JavaUtils}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/1/13.
  */

class TwoOrderKey(val first: String, val second: Int) extends Ordered[TwoOrderKey] with Serializable {
    def compare(other: TwoOrderKey): Int = {

        if (!this.first.equals(other.first)) {
            if (this.first > other.first) 1 else -1
        } else {
            this.second - other.second
        }
    }
}

object Simulator {

    /**
      * 思路: 1. 一个ip或者一段ip映射到多个uid
      *      2. 理论上正常用户而言会有2-5固定点, 即ip数量不会太多,
      * 如果ip是模拟的, 追踪多天的话, 一个uid对应的ip数量应该会很多
      *
      * @param args
      */
    def main(args: Array[String]): Unit = {
        //01010474

    }


    def saveSingleVersion(args: Array[String]) = {
        val Array(dayRange, partitions) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val startRunPath = s"/user/iflyol/ossp/input/$dayRange/StartRunLog"
        val contactPath = s"/user/iflyms/flume/$dayRange/*/*acdata*{0,1,2,3,4,5,6,7,8,9}/"
        val usePath = s"/user/iflyms/inter/UseInfo/$dayRange"
        val installPath = s"/user/iflyms/inter/AppInstallInfo/$dayRange"

        /* optional string imsi = 2; 111
         optional string imei = 3; 105
         optional string phoneType = 4;  110 //手机型号
         optional string version = 5;  108 //输入法版本*/
        //IP信息
        val startRun = CommonService.readLzoFile(sc, startRunPath)
            .map(CommonService.MapLoader) //101:uid 115:caller 124:remoteIp
            .filter(m => m.getOrElse("101", "").length >= 15)
            .map(m => {
                val uid = m.getOrElse("101", "")
                val fdf = m.getOrElse("107", "")
                val ip = m.getOrElse("124", "")
                val caller = m.getOrElse("115", "")
                val imsi = m.getOrElse("111", "")
                val imei = m.getOrElse("105", "")
                val phoneType = m.getOrElse("110", "").trim().split("\\|")(0)
                val version = m.getOrElse("108", "")
                val ctm = m.getOrElse("125", "")

                (uid, (fdf, ip, caller, imsi, imei, phoneType, version, ctm))
            })
            .reduceByKey {
                case (x, y) => if (x._8 > y._8) x else y
            }
            .map {
                case (uid, (fdf, ip, caller, imsi, imei, phoneType, version, ctm)) =>
                    (uid, fdf, ip, caller, imsi, imei, phoneType, version)
            }

        val strMsgRdd = startRun.map {
            case (uid, fdf, ip, caller, imsi, imei, phoneType, version) =>
                //string data
                var map: Map[String, Array[Byte]] = Map("cf:fdf" -> Bytes.toBytes(fdf),
                    "cf:imei" -> Bytes.toBytes(imei),
                    "cf:phonetype" -> Bytes.toBytes(phoneType),
                    "cf:version" -> Bytes.toBytes(version))
                if (imsi.length > 0) map = map.+("cf:imsi" -> Bytes.toBytes(imsi)) //存在很多imsi为空情况

                //ip
                val ipBuf = InputUser.IP.newBuilder()
                    .setLatestIp(ip)
                    .build()
                map = map.+("cf:ip" -> ipBuf.toByteArray)

                //self caller
                if (CommonService.isPhoneNum(caller)) {
                    val callBuf = InputUser.Caller.newBuilder()
                        .setCaller(caller.toLong)
                        .build()
                    map = map.+("cf:caller" -> callBuf.toByteArray)
                }
                (uid, map)
        }
        HBaseUtil.hbaseWriteBytes("iflyrd:InputNewUserInfo", strMsgRdd)

        //正常号码、异常号码信息
        val contact = sc.textFile(contactPath)
            .map(CommonService.MapLoader)
            .filter(m => (m.getOrElse("uid", "").length >= 15) && (m.getOrElse("flumedesds", "").length > 0))
            .map {
                map => (map.getOrElse("uid", ""), CommonService.getPhoneNums(map.getOrElse("flumedesds", "")))
            }
            .filter {
                case (uid, (nor, exp)) => !(nor.isEmpty && exp.isEmpty)
            }
            .reduceByKey {
                case (x, y) => if (x._1.size > y._1.size) x else y
            }
            .map {
                case (uid, (callers, excepts)) => {
                    val expObj = excepts.map(item => {
                        InputUser.Except.newBuilder()
                            .setExcept(item)
                            .build()
                    })

                    val calObj = callers.map(item => {
                        InputUser.Caller.newBuilder()
                            .setCaller(item.toLong)
                            .build()
                    })

                    import collection.JavaConversions._
                    val conObj = InputUser.Contact.newBuilder()
                        .addAllCallers(calObj)
                        .addAllExcepts(expObj)
                        .build()
                    (uid, Map("cf:contact" -> conObj.toByteArray))
                }
            }
        HBaseUtil.hbaseWriteBytes("iflyrd:InputNewUserInfo", contact)


        //app使用信息
        val use = sc.textFile(usePath).map(_.split("~", -1))
            .filter(arr => (arr.length == 8) && (arr(0).length >= 15) && (arr(4).length > 0)) //uid & pkg check
            .map {
            case Array(uid, imei, date, colDays, pkg, dec, bin, appName) => {
                val app = InputUser.App.newBuilder()
                    .setPkg(pkg)
                    .setName(appName)
                    .setCollectDays(colDays.toInt)
                    .setActiveDays(bin.count(x => x == '1'))
                    .build()
                (uid, app.toByteArray)
            }
        }
            .reduceByKey {
                case (x, y) => {
                    val xObj = InputUser.App.parseFrom(x)
                    val yObj = InputUser.App.parseFrom(y)
                    val zObj = InputUser.AppList.newBuilder()
                        .addApps(xObj)
                        .addApps(yObj)
                        .build()
                    zObj.toByteArray
                }
            }
            .map {
                case (uid, arr) => (uid, Map("cf:use" -> arr))
            }
        HBaseUtil.hbaseWriteBytes("iflyrd:InputNewUserInfo", use)

        //app安装信息
        val install = sc.textFile(installPath).map(_.split("~", -1))
            .filter(arr => (arr.length == 5) && (arr(0).length >= 15) && (arr(2).length > 0)) //uid & pkg check
            .map {
            case Array(uid, imei, pkg, appName, version) => {
                val app = InputUser.App.newBuilder()
                    .setPkg(pkg)
                    .setName(appName)
                    .build()
                (uid, app.toByteArray)
            }
        }
            .reduceByKey {
                case (x, y) => {
                    val xObj = InputUser.App.parseFrom(x)
                    val yObj = InputUser.App.parseFrom(y)
                    val zObj = InputUser.AppList.newBuilder()
                        .addApps(xObj)
                        .addApps(yObj)
                        .build()
                    zObj.toByteArray
                }
            }
            .map{
                case (uid, arr) => (uid, Map("cf:install" -> arr))
            }
        HBaseUtil.hbaseWriteBytes("iflyrd:InputNewUserInfo", install)

        //unpersist & stop
        startRun.unpersist()
        sc.stop()
    }


    def saveHbaseJoinVersion(args: Array[String]): Unit = {
        val Array(dayRange, partitions) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        //        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 注册要序列化的自定义类型。
        //        conf.registerKryoClasses(Array(classOf[InputUser]))
        val sc = new SparkContext(conf)

        val startRunPath = s"/user/iflyol/ossp/input/$dayRange/StartRunLog"
        val contactPath = s"/user/iflyms/flume/$dayRange/*/*acdata*{0,1,2,3,4,5,6,7,8,9}/"
        val usePath = s"/user/iflyms/inter/UseInfo/$dayRange"
        val installPath = s"/user/iflyms/inter/AppInstallInfo/$dayRange"

        /* optional string imsi = 2; 111
         optional string imei = 3; 105
         optional string phoneType = 4;  110 //手机型号
         optional string version = 5;  108 //输入法版本*/
        //IP信息
        val startRun = CommonService.readLzoFile(sc, startRunPath)
            .map(CommonService.MapLoader) //101:uid 115:caller 124:remoteIp
            .filter(m => m.getOrElse("101", "").length >= 15)
            .map(m => {
                val uid = m.getOrElse("101", "")
                val fdf = m.getOrElse("107", "")
                val ip = m.getOrElse("124", "")
                val caller = m.getOrElse("115", "")
                val imsi = m.getOrElse("111", "")
                val imei = m.getOrElse("105", "")
                val phoneType = m.getOrElse("110", "").trim().split("\\|")(0)
                val version = m.getOrElse("108", "")
                val ctm = m.getOrElse("125", "")

                (uid, (fdf, ip, caller, imsi, imei, phoneType, version, ctm))
            })
            .reduceByKey {
                case (x, y) => if (x._8 > y._8) x else y
            }
            .map {
                case (uid, (fdf, ip, caller, imsi, imei, phoneType, version, ctm)) =>
                    (uid, fdf, ip, caller, imsi, imei, phoneType, version)
            }

//        val strMsgRdd = startRun.map {
//            case (uid, fdf, ip, caller, imsi, imei, phoneType, version) =>
//                val strMap: Map[String, String] = Map("cf:fdf" -> fdf,
//                    "cf:imei" -> imei,
//                    "cf:phonetype" -> phoneType,
//                    "cf:version" -> version)
//                if (imsi.length > 0) strMap.+("cf:imsi" -> imsi) //存在很多imsi为空情况
//                (uid, strMap)
//        }
//        HBaseUtil.hbaseWrite("iflyrd:InputNewUserInfo", strMsgRdd)

        //正常号码、异常号码信息
        val contact = sc.textFile(contactPath)
            .map(CommonService.MapLoader)
            .filter(m => (m.getOrElse("uid", "").length >= 15) && (m.getOrElse("flumedesds", "").length > 0))
            .map {
                map => (map.getOrElse("uid", ""), CommonService.getPhoneNums(map.getOrElse("flumedesds", "")))
            }
            .filter {
                case (uid, (nor, exp)) => !(nor.isEmpty && exp.isEmpty)
            }
            .reduceByKey {
                case (x, y) => if (x._1.size > y._1.size) x else y
            }
            .map {
                case (uid, (callers, excepts)) => {
                    val expObj = excepts.map(item => {
                        InputUser.Except.newBuilder()
                            .setExcept(item)
                            .build()
                    })

                    val calObj = callers.map(item => {
                        InputUser.Caller.newBuilder()
                            .setCaller(item.toLong)
                            .build()
                    })

                    import collection.JavaConversions._
                    val conObj = InputUser.Contact.newBuilder()
                        .addAllCallers(calObj)
                        .addAllExcepts(expObj)
                        .build()
                    (uid, conObj.toByteArray)
                }
            }

        //app使用信息
        val use = sc.textFile(usePath).map(_.split("~", -1))
            .filter(arr => (arr.length == 8) && (arr(0).length >= 15) && (arr(4).length > 0)) //uid & pkg check
            .map {
            case Array(uid, imei, date, colDays, pkg, dec, bin, appName) => {
                val app = InputUser.App.newBuilder()
                    .setPkg(pkg)
                    .setName(appName)
                    .setCollectDays(colDays.toInt)
                    .setActiveDays(bin.count(x => x == '1'))
                    .build()
                (uid, app.toByteArray)
            }
        }
            .reduceByKey {
                case (x, y) => {
                    val xObj = InputUser.App.parseFrom(x)
                    val yObj = InputUser.App.parseFrom(y)
                    val zObj = InputUser.AppList.newBuilder()
                        .addApps(xObj)
                        .addApps(yObj)
                        .build()
                    zObj.toByteArray
                }
            }

        //app安装信息
        val install = sc.textFile(installPath).map(_.split("~", -1))
            .filter(arr => (arr.length == 5) && (arr(0).length >= 15) && (arr(2).length > 0)) //uid & pkg check
            .map {
            case Array(uid, imei, pkg, appName, version) => {
                val app = InputUser.App.newBuilder()
                    .setPkg(pkg)
                    .setName(appName)
                    .build()
                (uid, app.toByteArray)
            }
        }
            .reduceByKey {
                case (x, y) => {
                    val xObj = InputUser.App.parseFrom(x)
                    val yObj = InputUser.App.parseFrom(y)
                    val zObj = InputUser.AppList.newBuilder()
                        .addApps(xObj)
                        .addApps(yObj)
                        .build()
                    zObj.toByteArray
                }
            }

        //save procobuf data
        val procobufData = startRun.map {
            case (uid, fdf, ip, caller, imsi, imei, phoneType, version) => (uid, (fdf, ip, caller, imsi, imei, phoneType, version))
        }
            .leftOuterJoin(contact)
            .map {
                case (uid, (startRunFields, opt)) => (uid, (startRunFields, opt))
            }
            .leftOuterJoin(use)
            .map {
                case (uid, ((startRunFields, conOpt), useOpt)) => (uid, (startRunFields, conOpt, useOpt))
            }
            .leftOuterJoin(install)
            .map {
                case (uid, (((fdf, ip, caller, imsi, imei, phoneType, version), conOpt, useOpt), insOpt)) => {
                    //string data
                    var map: Map[String, Array[Byte]] = Map("cf:fdf" -> Bytes.toBytes(fdf),
                        "cf:imei" -> Bytes.toBytes(imei),
                        "cf:phonetype" -> Bytes.toBytes(phoneType),
                        "cf:version" -> Bytes.toBytes(version))
                    if (imsi.length > 0) map = map.+("cf:imsi" -> Bytes.toBytes(imsi)) //存在很多imsi为空情况

                    //ip
                    val ipBuf = InputUser.IP.newBuilder()
                        .setLatestIp(ip)
                        .build()
                    map = map.+("cf:ip" -> ipBuf.toByteArray)

                    //self caller
                    if (CommonService.isPhoneNum(caller)) {
                        val callBuf = InputUser.Caller.newBuilder()
                            .setCaller(caller.toLong)
                            .build()
                        map = map.+("cf:caller" -> callBuf.toByteArray)
                    }

                    //contact
                    conOpt match {
                        case Some(con) => map = map.+("cf:contact" -> con)
                        case None => {} //just coding habit
                    }

                    //use
                    useOpt match {
                        case Some(useList) => map = map.+("cf:use" -> useList)
                        case None => {}
                    }

                    //install
                    insOpt match {
                        case Some(inst) => map = map.+("cf:install" -> inst)
                        case None => {}
                    }
                    (uid, map)
                }
            }

        HBaseUtil.hbaseWriteBytes("iflyrd:InputNewUserInfo", procobufData)
        sc.stop()
    }

    def version_ip(args: Array[String]) = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        //360 01430734
        //coobee 01010474
        val Array(oneDay, tag) = args
        val channels = sc.broadcast(Set("01430734", "01010474"))
        val day = oneDay.replaceAll("\\*", "")

        val news = s"/user/iflyol/ossp/inter/EtlSys/NewUserInfo/$oneDay"
        val startRun = s"/user/iflyol/ossp/input/$oneDay/StartRunLog"

        // data load & transport
        val dayNewUsers = CommonService.readLzoFile(sc, news)
            .map(_.split("~", -1))
            .filter(x => (x.length >= 6) && (x(0).length > 0)
                && x(1).toLowerCase().equals("100ime")) //bizid
            .map {
            case Array(uid, imei, os, chId, version, date) => (uid, chId)
        }

        val callerInfo = CommonService.readLzoFile(sc, startRun)
            .map(CommonService.MapLoader) //101:uid 115:caller 124:remoteIp
            .map(m => (m.getOrElse("101", ""), m.getOrElse("124", "")))
            .filter {
                case (uid, ip) => uid.length > 0 && CommonService.isPublicIP(ip)
            }

        val middle = dayNewUsers.join(callerInfo)
            .distinct()
            .map {
                case (uid, (chId, ip)) => ((chId, ip), (1, uid))
            }
            .reduceByKey {
                case (x, y) => (x._1 + y._1, Seq(x._2, y._2).mkString(":"))
            }
            .cache()

        //save middle data
        middle.map {
            case ((chId, caller), (uidCnt, uid)) => (new TwoOrderKey(chId, uidCnt), (caller, uid))
        }
            .filter(x => x._1.second > 1)
            .sortByKey(false)
            .map {
                case (obj, (caller, uid)) => Seq(obj.first, caller, obj.second).mkString("~")
            }
            .coalesce(8)
            .saveAsTextFile(s"/user/iflyrd/work/jjhu/simulator/ip/details/$day")

        //save result data
        middle.map {
            case ((chId, caller), (uidCnt, uid)) => (chId, (1, uidCnt))
        }
            .reduceByKey {
                case (x, y) => (x._1 + y._1, x._2 + y._2)
            }
            .map {
                case (chId, (callerCnt, uidCnt)) => (chId, callerCnt, uidCnt, uidCnt - callerCnt)
            }
            .sortBy(-_._4) //(用户量 - 号码量)
            .map {
            case (chId, callerCnt, uidCnt, abs) => Seq(chId, callerCnt, uidCnt, abs).mkString("~")
        }
            .coalesce(1)
            .saveAsTextFile(s"/user/iflyrd/work/jjhu/simulator/ip/result/$day")


        middle.unpersist()
        sc.stop()
    }

    /**
      * 反作弊二次探索
      * 1. 认为startRunLog caller信息准确, 运营商返回数据, 无法被篡改
      * 2. 如果存在作弊行为, 认为一个caller可能对应多个uid
      * 3. details：chId~caller~uidCnt~uids 明细数据
      * 4. result：chId~callerCnt~uidCnt~diff
      *
      * @param args
      */
    def version_caller(args: Array[String]) = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        //360 01430734
        //coobee 01010474
        val Array(oneDay, isNull, tag) = args
        val channels = sc.broadcast(Set("01430734", "01010474"))
        val keyMap = sc.broadcast(Map("caller" -> "115", "ua" -> "110", "imei" -> "105", "imsi" -> "111"))
        val day = oneDay.replaceAll("\\*", "")

        val news = s"/user/iflyol/ossp/inter/EtlSys/NewUserInfo/$oneDay"
        val startRun = s"/user/iflyol/ossp/input/$oneDay/StartRunLog"

        // data load & transport
        val dayNewUsers = CommonService.readLzoFile(sc, news)
            .map(_.split("~", -1))
            .filter(x => (x.length >= 6) && (x(0).length > 0)
                && x(1).toLowerCase().equals("100ime")) //bizid
            .map {
            case Array(uid, imei, os, chId, version, date) => (uid, chId)
        }

        val callerInfo = CommonService.readLzoFile(sc, startRun)
            .map(CommonService.MapLoader) //101:uid 115:caller 110:ua 105:imei 111:imsi
            .map(m => (m.getOrElse("101", ""), m.getOrElse(keyMap.value.getOrElse(tag, "115"), "")))
            .filter {
                case (uid, caller) => uid.length > 0 && caller.length > 0
            }

        val middle = dayNewUsers.join(callerInfo)
            .distinct() //数据量少一些
            .map {
            case (uid, (chId, caller)) => ((chId, caller), (1, uid))
        }
            .reduceByKey {
                case (x, y) => (x._1 + y._1, Seq(x._2, y._2).mkString(":"))
            }
            .cache()

        //save middle data
        middle.map {
            case ((chId, caller), (uidCnt, uid)) => (chId, caller, uidCnt, uid)
        }
            .filter(_._3 > 1)
            .map {
                case (chId, caller, uidCnt, uid) => (new TwoOrderKey(chId, uidCnt), (caller, uid))
            }
            .sortByKey(false)
            .map {
                case (obj, (caller, uid)) => Seq(obj.first, caller, obj.second).mkString("~")
            }
            .coalesce(8)
            .saveAsTextFile(s"/user/iflyrd/work/jjhu/simulator/$tag/details/${day}_$isNull")

        //save result data
        middle.map {
            case ((chId, caller), (uidCnt, uid)) => (chId, (1, uidCnt))
        }
            .reduceByKey {
                case (x, y) => (x._1 + y._1, x._2 + y._2)
            }
            .map {
                case (chId, (callerCnt, uidCnt)) => (chId, callerCnt, uidCnt, uidCnt - callerCnt)
            }
            .sortBy(-_._4) //(用户量 - 号码量)
            .map {
            case (chId, callerCnt, uidCnt, abs) => Seq(chId, callerCnt, uidCnt, abs).mkString("~")
        }
            .coalesce(1)
            .saveAsTextFile(s"/user/iflyrd/work/jjhu/simulator/$tag/result/${day}_$isNull")


        middle.unpersist()
        sc.stop()
    }

    /**
      * Target: 分析360、coobee渠道下, 用户app安装、使用信息
      * Practice:
      * 1. 使用md5算法将app pkg映射为int, 取和作为用户app集合唯一特征
      * 2. (chId, pkgNum, md5)作为key, 聚合同一渠道下app使用、安装信息完全一致的用户, 用以评估模拟器作弊可能性
      *
      * @param args
      */
    def version_app(args: Array[String]) = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        //360 01430734
        //coobee 01010474
        val Array(oneDay) = args
        val channels = sc.broadcast(Set("01430734", "01010474"))

        val news = s"/user/iflyol/ossp/inter/EtlSys/NewUserInfo/$oneDay"
        val use = s"/user/iflyms/inter/UseInfo/$oneDay"
        val install = s"/user/iflyms/inter/AppInstallInfo/$oneDay"

        // data load & transport
        val dayNewUsers = CommonService.readLzoFile(sc, news)
            .map(_.split("~", -1))
            .filter(x => (x.length >= 6) && (x(0).length > 0))
            .map {
                case Array(uid, imei, os, chId, version, date) => (uid, chId)
            }
            .filter(x => channels.value.contains(x._2))
            .cache()

        val dayInstall = sc.textFile(install).map(_.split("~", -1))
            .filter(x => (x.length >= 5) && (x(0).length > 0))
            .map {
                arr => {
                    /*if (arr.length == 3) {
                        //pkg为none
                        (arr(0), (1, -1)) //需要确认none值具体情况, 1)有app,但没取到,赋权重-1  2)none只是日志记录问题,赋权重0
                    } else {
                        (arr(0), (1, arr(2).length)) //(uid, (pkgNum, pkgLen))
                    }*/

                    (arr(0), (1, JavaUtils.hashEncodeUid(arr(2)), arr(2))) //(uid, (pkgNum, md5Int, pkg))
                }
            }
            .reduceByKey {
                case (x, y) => (x._1 + y._1, x._2 + y._2, Seq(x._3, y._3).mkString(":"))
            }

        val dayUse = sc.textFile(use).map(_.split("~"))
            .filter(x => (x.length >= 8) && (x(0).length > 0)) //uid.length > 0
            .map {
            arr => (arr(0), (1, JavaUtils.hashEncodeUid(arr(4)), arr(4))) //(uid, (pkgNum, md5, pkg))
        }
            .reduceByKey {
                case (x, y) => (x._1 + y._1, x._2 + y._2, Seq(x._3, y._3).mkString(":"))
            }

        // deal with & save install data
        //        saveMD5(dayNewUsers, dayInstall, "install", oneDay)

        // deal with & save use data
        saveMD5(dayNewUsers, dayUse, "use", oneDay)

        dayNewUsers.unpersist()
        sc.stop()
    }

    def saveMD5(chInfo: RDD[(String, String)],
                appInfo: RDD[(String, (Int, Int, String))],
                appSource: String,
                date: String) = {
        val total = chInfo.join(appInfo)
            .map {
                case (uid, (chId, (pkgNum, md5, pkg))) => ((chId, pkgNum, md5), (1, pkg))
            }
            .reduceByKey {
                case (x, y) => (x._1 + y._1, x._2)
            }
            .cache()

        saveInHdfsMD5(total, "01430734", s"/user/iflyrd/work/jjhu/simulator/$date/${appSource}_360")
        saveInHdfsMD5(total, "01010474", s"/user/iflyrd/work/jjhu/simulator/$date/${appSource}_coobee")

        total.unpersist()
    }

    def save(chInfo: RDD[(String, String)],
             appInfo: RDD[(String, (Int, Int))],
             appSource: String,
             date: String) = {
        val total = chInfo.join(appInfo)
            .map {
                case (uid, (chId, (pkgNum, pkgLen))) => ((chId, pkgNum, pkgLen), 1)
            }
            .reduceByKey(_ + _)
            .cache()

        saveInHdfs(total, "01430734", s"/user/iflyrd/work/jjhu/simulator/$date/${appSource}_360")
        saveInHdfs(total, "01010474", s"/user/iflyrd/work/jjhu/simulator/$date/${appSource}_coobee")

        total.unpersist()
    }


    def saveInHdfsMD5(data: RDD[((String, Int, Int), (Int, String))],
                      chId: String,
                      path: String) = {
        data.filter(x => x._1._1.equals(chId))
            .sortBy(x => -x._2._1)
            .map {
                case ((chId, pkgNum, md5), (count, pkg)) => Seq(chId, pkgNum, count, pkg).mkString("~")
            }
            .coalesce(1)
            .saveAsTextFile(path)
    }

    def saveInHdfs(data: RDD[((String, Int, Int), Int)],
                   chId: String,
                   path: String) = {
        data.filter(x => x._1._1.equals(chId))
            .sortBy(x => -x._2)
            .map {
                case ((chId, pkgNum, pkgLen), count) => Seq(chId, pkgNum, pkgLen, count).mkString("~")
            }
            .coalesce(1)
            .saveAsTextFile(path)
    }
}
