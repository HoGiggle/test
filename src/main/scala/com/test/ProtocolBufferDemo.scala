package com.test

import com.protobf.ComIflytekCtrPerson
import com.protobf.ComIflytekCtrPerson.Person
import com.util.HBaseUtil
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Administrator on 2015/12/3.
  */
object ProtocolBufferDemo {

    def parsResultRDD(columnsFamily: String, resultRDD: RDD[scala.Tuple2[ImmutableBytesWritable, Result]]): RDD[(String, Map[String, Array[Byte]])] = {
        val result = resultRDD.map(x => {
            //      get value of rowkey
            val valueMap = x._2.rawCells()
            //      generate MAP to contain result
            val resultMap = scala.collection.mutable.Map[String, Array[Byte]]()

            //      get rowkey and put in result map
            //      resultMap += ("rowkey" -> Bytes.toString(x._2.getRow))
            //      iterate all the value and put in result map
            valueMap.foreach(rawKV => {
                resultMap += (columnsFamily + ":" + Bytes.toString(CellUtil.cloneQualifier(rawKV)) -> CellUtil.cloneValue(rawKV))
            })
            (Bytes.toString(x._2.getRow), resultMap.toMap)
        })
        result
    }


    def scanFromHBase(sc: SparkContext, table: String,
                      startRow: String,
                      endRow: String,
                      columns: Set[String] = Set(),
                      columnsFamily: String = "cf"): RDD[(String, Map[String, Array[Byte]])] = {
        //    set the related conf of HBase
        val conf = HBaseUtil.HBaseGrootConf()
        //    set table name to scan
        conf.set(TableInputFormat.INPUT_TABLE, table)
        //    config scan object
        val scan = HBaseUtil.configScan(startRow, endRow)
        //    if no columns, set the column family
        if (columns.size == 0) {
            scan.addFamily(Bytes.toBytes(columnsFamily))
        } else {
            //      else set the columns to scan
            columns.foreach(x => scan.addColumn(Bytes.toBytes(columnsFamily), Bytes.toBytes(x)))
        }
        //    change the scan to String
        val proto = ProtobufUtil.toScan(scan)
        val scanEncode = Base64.encodeBytes(proto.toByteArray)
        //    config the scan into conf
        conf.set(TableInputFormat.SCAN, scanEncode)
        //  get the result HBaseRDD
        val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])
        //  parse result of HBaseRDD into RDD[Map[String,String]]
        parsResultRDD(columnsFamily, hbaseRDD)
    }

    def main(args: Array[String]) {
        // 设置sparkContext
        val conf = new SparkConf().setAppName("UidIndex")
            .setMaster("local")
        val sc = new SparkContext(conf)
//        HBaseUtil.setHBaseConf("192.168.45.150", "192.168.45.150:16000")
        // 生成一个用户的对象信息
        val app1 = ComIflytekCtrPerson.App.newBuilder()
            .setPkg("com.qq")
            .setName("QQ")
            .build()

        val app2 = ComIflytekCtrPerson.App.newBuilder()
            .setPkg("com.weixin")
            .setName("WEIXIN")
            .build()

        val person = ComIflytekCtrPerson.Person.newBuilder()
            .setAcctID(1)
            .setPasswd("1989")
            .addIndex(0)
            .addValue(0)
            .addIndex(1)
            .addValue(1)
            .addApps(app1).addApps(app2)
            .build()
        // 对象转成Array[Byte]
//        val arr = person.toByteArray
        val val1 = Array(("rowkey0", Map("cf:value" -> person)))
        val hbaseRDD = sc.parallelize(val1, 1)
            .reduceByKey{
                case (x, y) => x
            }
            .map(x => (x._1, x._2.map(y => (y._1, y._2.toByteArray))))
        // 写入HBase
        HBaseUtil.hbaseWriteBytes("iflyrd:test", hbaseRDD)
        // 从HBase读入写入的对象
        val ret = scanFromHBase(sc, "iflyrd:test", "rowkey0", "rowkey3", Set("value"), "cf")
        ret.foreach({
            case (rowkey, value) => {
                val bytesValue = value.get("cf:value")
                // 对person对象解析
                val person = Person.parseFrom(bytesValue.get)
                println(rowkey + person.getAcctID + "~" + person.getPasswd)
                // 获取repeated变量数组
                person.getIndexList.toArray.foreach(p => println("index" + p))
                person.getValueList.toArray.foreach(p => println("value" + p))
                for (app <- person.getAppsList.toArray){
                    app match {
                        case x:ComIflytekCtrPerson.App => println(x.getPkg)
                    }
                }

            }
        })
        sc.stop()
    }
}
