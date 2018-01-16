package com.cheat

import com.input.cheat.{JaccardSim, User}
import com.protobf.InputUser
import com.util.HBaseUtil
import org.apache.commons.math3.ml.clustering.DBSCANClusterer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2017/2/17.
  */

object InputDBScan {
    implicit def long2Double(lArray: Array[Long]): Array[Double] = lArray.map(l => l.toDouble)

    def main(args: Array[String]): Unit = {
        runDBScanLocal(args)
//        runDBScanCluster(args)
    }

    def runDBScanCluster(args: Array[String]) = {
        val Array(fdf, eps, minPts) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)

        val dfUids = HBaseUtil.scanByValueFilter(sc, "iflyrd:InputNewUserInfo", "cf", "fdf", fdf)
            .map{
                case (uid, valueMap) => uid
            }
            .cache()
        val dfMsg = HBaseUtil.batchGetFromHbase(dfUids, 1000, "iflyrd:InputNewUserInfo")
            //            .filter{
            //                case (uid, resMap) => resMap.contains("cf:contact")
            //            }
            .map{
            case (uid, resMap) => {
                val contactOpt = resMap.get("cf:contact")
                import collection.JavaConversions._
                val contacts = contactOpt match {
                    case Some(x) => InputUser.Contact.parseFrom(x).getCallersList
                        .map(caller => caller.getCaller)
                    case None => Seq()
                }
                (uid, contacts)
            }
        }
            .cache()

        // (contact, index) broadcast
        val contactIndex = sc.broadcast(dfMsg.map(_._2).flatMap(seq => seq)
            .distinct()
            .zipWithIndex()
            .collect()
            .toMap)

        // data for cluster
        val clusterData = dfMsg.map{
            case (uid, contacts) => {
                if (contacts.isEmpty){
                    (uid, Array(-1L))  //如果没有联系人信息, 赋值默认index为-1
                } else {
                    (uid, contacts.map(caller => contactIndex.value.getOrElse(caller, -2L)).toArray)
                }
            }
        }.cache()

        //save middle data
        clusterData.map{
                case (uid, contacts) => Seq(uid, contacts.mkString(":")).mkString("~")
            }
            .coalesce(1).saveAsTextFile(s"/user/iflyrd/work/jjhu/simulator/middle/$fdf")

        val dbscan = new DBSCANClusterer[User](eps.toDouble, minPts.toInt, new JaccardSim)
        import collection.JavaConversions._
        val userList = clusterData.collect()
            .map{
                case (uid, contactIndexes) => new User(uid, contactIndexes)
            }
        val result = dbscan.cluster(userList.toList)
        for (index <- 0 until result.size()){
            println("All cluster is : " + result.size())
            println("Cluster*************" + index)
            for (point <- result.get(index).getPoints){
                println(point.getUid)
            }
        }

        dfMsg.unpersist()
        clusterData.unpersist()
        sc.stop()
    }

    def runDBScanLocal(args: Array[String]) = {
        val Array(eps, minPts) = args
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)


        val clusterData = sc.objectFile[String]("/user/iflyrd/work/jjhu/simulator/middle/part-00000") //file:///E:\data\contact
            .map(_.split("~"))
            .filter(_.length == 2)
            .map{
                case Array(uid, contacts) => {
                    val tmp = contacts.split(":", -1).map(index => index.toDouble)
                    (1, new User(uid, tmp))
                }
            }
            .groupByKey()
            .map{
                case (tmp, user) => {
                    val dbscan = new DBSCANClusterer[User](eps.toDouble, minPts.toInt, new JaccardSim)
                    import collection.JavaConversions._
                    val result = dbscan.cluster(user.toList)
                    println("All cluster is : " + result.size())

                    for (index <- 0 until result.size()){
                        println("Cluster*************" + index)
                        for (point <- result.get(index).getPoints){
                            println(point.getUid)
                        }
                    }
                    (tmp, result.size())
                }
            }
        for ((groupId, clusterSize) <- clusterData.collect()){
            println("GroupId = " + groupId + ", ClusterSize = " + clusterSize)
        }
        sc.stop()
    }
}
