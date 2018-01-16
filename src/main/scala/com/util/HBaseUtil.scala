package com.util

import java.util

import com.iflytek.GetHbaseScan.GetScan
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import unicredit.spark.hbase.HBaseConfig

import scala.collection.JavaConversions._
import scala.collection.mutable


/**
  * Created by jjhu on 2017/2/7.
  */
object HBaseUtil {
    val quorumIp = "172.16.69.14,172.16.69.15,172.16.69.16"
    val rootDir = "172.16.67.21:16000"
    var hbaseConf = HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum", quorumIp)
    hbaseConf.set("hbase.master", rootDir)

    private var connection:HConnection = _
    private var isConnectionInit:Boolean = false

    case class Config(
                         quorum:String,
                         rootdir:String)
    implicit val hbase_config = HBaseConfig(Config(quorumIp, rootDir))
    /*
      $func config HBase master and zookeeper
    */
    def HBaseGrootConf():Configuration = {
        hbaseConf
    }

    def setHBaseConf(hconf:Configuration): Unit ={
        hbaseConf = hconf
        if(!isConnectionInit){
            connection = HConnectionManager.createConnection(hbaseConf)
            isConnectionInit = true
        }else {
            connection.close()
            connection = HConnectionManager.createConnection(hbaseConf)
        }
    }

    @Deprecated
    def setHBaseConf(ip:String, master:String): Unit ={
        hbaseConf.set("hbase.zookeeper.quorum", ip)
        hbaseConf.set("hbase.master", master)
        if(!isConnectionInit){
            connection = HConnectionManager.createConnection(hbaseConf)
            isConnectionInit = true
        }else {
            connection.close()
            connection = HConnectionManager.createConnection(hbaseConf)
        }
    }

    def setHBaseConf(zkQuorum:String): Unit ={
        hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
        if(!isConnectionInit){
            connection = HConnectionManager.createConnection(hbaseConf)
            isConnectionInit = true
        }else {
            connection.close()
            connection = HConnectionManager.createConnection(hbaseConf)
        }
    }

    def closeConnection(): Unit ={
        if(!connection.isClosed) {
            connection.close()
            isConnectionInit = false
        }
    }

    def convert(id: String, values: Map[String, String]) = {
        def bytes(s: String) = Bytes.toBytes(s)

        val put = new Put(bytes(id))
        var empty = true
        for {(familyAndColumn, content) <- values} {
            empty = false
            put.add(bytes(familyAndColumn.split(":")(0)), bytes(familyAndColumn.split(":")(1)), content.getBytes)
        }
        if (empty) None else Some(new ImmutableBytesWritable, put)
    }

    /*
    @func write data in HBase table
    @table String, table name
    @rdd Map[String, String]
     */
    def hbaseWrite(table: String, rdd: RDD[(String, Map[String, String])]): Unit = {

        val conf = HBaseGrootConf()

        conf.set(TableOutputFormat.OUTPUT_TABLE, table)

        val job = new Job(conf, this.getClass.getName.split('$')(0))
        job.setOutputFormatClass(classOf[TableOutputFormat[String]])

        rdd.flatMap({ case (k, v) => convert(k, v)})
            .saveAsNewAPIHadoopDataset(job.getConfiguration)
    }

    def configScan(startRow:String, endRow:String):Scan = {
        val scan = new Scan()
        scan.setStartRow(Bytes.toBytes(startRow))
        scan.setStopRow(Bytes.toBytes(endRow))
        scan
    }

    def convertBytes(id: String, values: Map[String, Array[Byte]]) = {
        def bytes(s: String) = Bytes.toBytes(s)

        val put = new Put(bytes(id))
        var empty = true
        for {(familyAndColumn, content) <- values} {
            empty = false
            put.add(bytes(familyAndColumn.split(":")(0)), bytes(familyAndColumn.split(":")(1)), content)
        }
        if (empty) None else Some(new ImmutableBytesWritable, put)
    }

    def hbaseWriteBytes(table: String, rdd: RDD[(String, Map[String, Array[Byte]])]): Unit = {

        val conf = HBaseUtil.HBaseGrootConf()

        conf.set(TableOutputFormat.OUTPUT_TABLE, table)

        val job = new Job(conf, this.getClass.getName.split('$')(0))
        job.setOutputFormatClass(classOf[TableOutputFormat[Array[Byte]]])

        rdd.flatMap{ case (k, v) => convertBytes(k, v)}
            .saveAsNewAPIHadoopDataset(job.getConfiguration)
    }

    def parsResultRDD(columnsFamily: String,
                      resultRDD: RDD[(ImmutableBytesWritable, Result)])
    :RDD[(String, Map[String, String])] = {
        val result = resultRDD.map(x => {
            //      get value of rowkey
            val valueMap = x._2.rawCells()
            //      generate MAP to contain result
            val resultMap = scala.collection.mutable.Map[String, String]()

            //      get rowkey and put in result map
            //      resultMap += ("rowkey" -> Bytes.toString(x._2.getRow))
            //      iterate all the value and put in result map
            valueMap.foreach(rawKV => {
                resultMap += (columnsFamily+ ":" +Bytes.toString(CellUtil.cloneQualifier(rawKV)) -> Bytes.toString(CellUtil.cloneValue(rawKV)))
            })
            (Bytes.toString(x._2.getRow), resultMap.toMap)
        })
        result
    }

    def scanByValueFilter(sc: SparkContext,
             tableName: String,
             family: String = "cf",
             qualifier: String,
             value: String)
             :RDD[(String, Map[String, String])] ={
        val conf = HBaseGrootConf()
        conf.set(TableInputFormat.INPUT_TABLE, tableName)

        //scan set
        val scan = new Scan()
        val singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family),
            Bytes.toBytes(qualifier), CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(Bytes.toBytes(value)))
        singleColumnValueFilter.setFilterIfMissing(true)
        scan.setFilter(singleColumnValueFilter)

        //change the scan to String
        val proto = ProtobufUtil.toScan(scan)
        val scanEncode = Base64.encodeBytes(proto.toByteArray)
        //config the scan into conf
        conf.set(TableInputFormat.SCAN, scanEncode)

        //get the result HBaseRDD
        val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])
        //  parse result of HBaseRDD into RDD[Map[String,String]]
        parsResultRDD(family, hbaseRDD)
    }


    /**
      * 用于mapPartition,使用新的API,根据批量rowkey获取记录
      * @param tableName
      * @param rowkeys
      * @return
      */
    def get(tableName: String,
            rowkeys: Iterator[String],
            batchSize: Int = 1000):Iterator[(String, Map[String,Array[Byte]])] = {
        val connection = ConnectionFactory.createConnection(hbaseConf)
        val table = connection.getTable(TableName.valueOf(tableName))
        val HbaseGetOperation = new HBaseCRUD(table)
        try{
            val gets = new util.ArrayList[String]()
            var res = List[(String,Map[String,Array[Byte]])]()

            while (rowkeys.hasNext) {
                gets.add(rowkeys.next)

                if (gets.size() == batchSize) {
                    val results = HbaseGetOperation.get(gets.toList)
                    res = res ++ results.toList
                    gets.clear()
                }
            }
            if (gets.size() > 0) {
                val results = HbaseGetOperation.get(gets.toList)
                res = res ++ results.toList
                gets.clear()
            }
            res.toIterator
        }finally {
            if(table != null) table.close()
            if(connection != null) connection.close()
        }
    }

    def batchGetFromHbase(rowKey: RDD[String],
                          batchGetHBasePartition: Int,
                          table: String)
    : RDD[(String,Map[String,Array[Byte]])] = {

        val result = rowKey.repartition(batchGetHBasePartition)
            .mapPartitions(HBaseUtil.get(table, _))
        result
    }

    def translateRDD(resultRDD: RDD[(ImmutableBytesWritable, Result)]): RDD[(String, Map[String, String])] = {
        resultRDD.map(x => {
            val resultMap = mutable.Map[String, String]()
            x._2.rawCells().foreach(rawKV => {
                resultMap += (Bytes.toString(CellUtil.cloneFamily(rawKV)) + ":" + Bytes.toString(CellUtil.cloneQualifier(rawKV)) -> Bytes.toString(CellUtil.cloneValue(rawKV)))
            })
            (Bytes.toString(x._2.getRow), resultMap.toMap)
        })
    }

    def hbaseRead(sc: SparkContext, table: String, columnList: String = "", optString: String = ""): RDD[(String, Map[String, String])] = {

        val conf = HBaseGrootConf()
        conf.set(TableInputFormat.INPUT_TABLE, table)
        val scan = new GetScan(columnList, optString).getScan()
        val proto = ProtobufUtil.toScan(scan)
        val scanEncode = Base64.encodeBytes(proto.toByteArray)
        conf.set(TableInputFormat.SCAN, scanEncode)
        val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        translateRDD(hbaseRDD)
    }
}