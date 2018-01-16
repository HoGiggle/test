package com.cheat

import com.protobf.InputUser
import com.util.{CommonService, HBaseUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/2/16.
  */
object InputDataLoad {
    def main(args: Array[String]): Unit = {
        val Array(dayRange) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val startRunPath = s"/user/iflyol/ossp/input/$dayRange/StartRunLog"
        val contactPath = s"/user/iflyms/flume/$dayRange/*/*acdata*{0,1,2,3,4,5,6,7,8,9}/"
        val usePath = s"/user/iflyms/inter/UseInfo/$dayRange"
        val installPath = s"/user/iflyms/inter/AppInstallInfo/$dayRange"

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
            .leftOuterJoin(install)
            .map {
                case (uid, ((startRunFields, conOpt), installOpt)) => (uid, (startRunFields, conOpt, installOpt))
            }
            .leftOuterJoin(use)
            .map {
                case (uid, (((fdf, ip, caller, imsi, imei, phoneType, version), conOpt, insOpt), useOpt)) => {
                    //string data
                    var map: Map[String, Array[Byte]] = Map(
                        "cf:fdf" -> Bytes.toBytes(fdf),
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
}
