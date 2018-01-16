package com.arts

import java.text.NumberFormat

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jjhu on 2016/11/16.
  */
object AlsModelComp {
    def main(args: Array[String]){
        val Array(path1, path2, out, comType) = args
        val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getName))

        val model1 = sc.objectFile[(Int, Array[(Int, Double)])](path1)
        val model2 = sc.objectFile[(Int, Array[(Int, Double)])](path2)

        val tmp1 = model1.map{
            case (uid, arr) => {
                (uid, arr.filter(x => x._1 != uid).map(x => x._1).toSet)
            }
        }
            .cache()
        val tmp2 = model2.map{
            case (uid, arr) => {
                (uid, arr.filter(x => x._1 != uid).map(x => x._1).toSet)
            }
        }
            .cache()

        println(path1 + ": " + tmp1.count())
        println(path2 + ": " + tmp2.count())

        tmp1.join(tmp2).map{
            case (uid, (set1, set2)) => {
                val d1 = (set1 & set2).size
                val d2 = (set1 | set2).size
                (decimalFormat(d1, d2, 2), 1)
            }
        }
            .reduceByKey(_ + _).sortByKey().map(x => Seq(x._1, x._2).mkString("~"))
            .saveAsTextFile(Seq(out, comType).mkString("/"))



        tmp1.unpersist()
        tmp2.unpersist()
        sc.stop()
    }

    def decimalFormat(d1:Int,
                      d2:Int,
                      decimalBit:Int):String={
        val nFormat = NumberFormat.getNumberInstance
        nFormat.setMaximumFractionDigits(decimalBit)
        var result = "0"
        if (d2 != 0) {
            result = nFormat.format(d1 * 1.0 / d2)
        }
        result
    }
}
