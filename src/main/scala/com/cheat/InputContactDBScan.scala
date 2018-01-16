//package com.cheat
//
//import breeze.linalg.{DenseMatrix, DenseVector}
//import nak.cluster.DBSCAN._
//import nak.cluster.GDBSCAN.Point
//import nak.cluster.{DBSCAN, GDBSCAN}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.{SparkConf, SparkContext}
///**
//  * Created by jjhu on 2017/2/20.
//  */
//
//case class User(uid: String, contactList: Array[String]) extends Serializable
//object InputContactDBScan {
//    def main(args: Array[String]) {
//        val Array(eps, minPts) = args
//        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//        val conf = new SparkConf().setAppName(this.getClass.getName)
//        val sc = new SparkContext(conf)
//        val clusterData = sc.objectFile[String]("/user/iflyrd/work/jjhu/simulator/middle/") //file:///E:\data\contact
//            .map(_.split("~"))
//            .filter(_.length == 2)
//            .map{
//                case Array(uid, contacts) => {
//                    val contactList = contacts.split(":", -1)
//                    (1, User(uid, contactList))
//                }
//            }
//            .groupByKey().mapValues(_.toArray)
//            .map{
//                case (groupId, points) => {
//                    val matrix = new DenseMatrix[User](points.length, 1, points)
//                    (groupId, matrix)
//                }
//            }
//
//        val clustersRdd = clusterData.mapValues(dbscan(eps.toDouble, minPts.toInt, _))
//
//        clustersRdd.collect().foreach{
//            case (uid, clusters) =>
//                clusters.foreach{
//                    case cluster =>
//                        val id = cluster.id
//                        val points = cluster.points
//                        println(Seq(uid, id, points.size, points.mkString(":")).mkString("~"))
//                }
//        }
//
//        println("all user count = " + clusterData.count())
//        println("all user count1 = " + clustersRdd.count())
//
//        sc.stop()
//    }
//
//
//    /**
//      *
//      * @param v points
//      * @return clusters
//      */
//    def dbscan(epsilon:Double,
//               minPoints:Int,
//               v: breeze.linalg.DenseMatrix[User])
//    :scala.Seq[nak.cluster.GDBSCAN.Cluster[User]] = {
//        val gdbscan = new GDBSCAN(
//            DBSCAN.getNeighbours(epsilon, jaccardDistance),
//            isCorePoint(minPoints)
//        )
//        val clusters = gdbscan cluster v
//        clusters
//    }
//
//
//    /**
//      * @param epsilon - minimum distance
//      * @param distance - for distance functions
//      */
//    def getNeighbours(epsilon: Double, distance: (DenseVector[Double], DenseVector[Double]) => Double)
//                     (point: Point[User], points: Seq[Point[User]]): Seq[Point[User]] = {
//        points.filter(neighbour => distance(neighbour.value.toArray(0)., point.value) < epsilon)
//    }
//
//    /**
//      * @param minPoints - minimal number of points to be a core point
//      */
//    def isCorePoint(minPoints: Double)(point: Point[Double], neighbours: Seq[Point[Double]]): Boolean = {
//        neighbours.size >= minPoints
//    }
//    def jaccardDistance(denseMatrix1: DenseVector[Double],
//                        denseMatrix2: DenseVector[Double])
//    :Double = {
//        println("jaccard coming!")
//        val array = denseMatrix1.map(_.toLong).toArray
//        val set = denseMatrix2.map(_.toLong).toArray.toSet
//        var intersectSize: Int = 0
//        var unionSize: Int = array.length + set.size
//
//        for (value <- array){
//            if (set.contains(value)){   //存在bug, 但理论上效率更高, denseMatrix1存在重复值时错误
//                intersectSize = intersectSize + 1
//                unionSize = unionSize - 1
//            }
//        }
//
//        println("intersectSize = " + intersectSize + ", unionSize = " + unionSize)
//        if (unionSize > 0){
//            return 1.0d - intersectSize * 1.0d / unionSize
//        }
//        return 1.0d
//    }
//}
