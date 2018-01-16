package com.util

import java.util

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, FamilyFilter, FilterList, QualifierFilter}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.collection.mutable

//将java的ArrayList转化为Scala的List

/**
 * Created by zqwu on 2015/12/3.
 * HBase的CRUD操作，包括get，put，delete，scan
 */
class HBaseCRUD(val table:Table) {

  /**
   * HBase的单条数据的put操作
   * param rowkey，eg. 100ime~android~20150101
   * param column,eg. Map("cf:pv" -> "100","cf:uv" -> "70")
   */
  def put(rowkey:String,column:Map[String,String]): Unit ={
    val put = cellToPut(rowkey,column) //将rowkey和column转换成Put
    table.put(put)
  }

  /**
   * HBase的批量数据的put操作
   * param arr eg. List((100ime~android~20150101,Map("cf:pv" -> "100")),(100ime~android~20150102,Map("cf:pv" -> "200")))
   */
  def put(arr:List[(String,Map[String,String])]): Unit ={
    val putList = arr.map(x => {
      cellToPut(x._1,x._2)
    })
    table.put(putList)
  }


  /**
   * 单个记录put，类似于shell put
   * param rowkey
   * param family
   * param qualifier
   * param value
   */
  def put(rowkey:String,family: String,qualifier:String,value:String): Unit ={
    put(Bytes.toBytes(rowkey),Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value))
  }

  /**
   * 单个记录put，类似于shell put
   * param rowkey
   * param family
   * param qualifier
   * param value
   */
  def put(rowkey:Array[Byte],family: Array[Byte],qualifier:Array[Byte],value:Array[Byte]): Unit ={
    val put = new Put(rowkey)
    put.addColumn(family,qualifier,value)
    table.put(put)
  }

  /**
   * 将rowkey和column记录转换成Put类
   * param rowkey
   * param column
   * return
   */
  private def cellToPut(rowkey:String,column:Map[String,String]):Put = {
    val put = new Put(Bytes.toBytes(rowkey))
    column.foreach(x => {
      val Array(family,qualifier) = x._1.split(":")
      put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(x._2))
    })
    put
  }

  /**
   * 根据rowkey获取相应的记录
   * param rowkey
   * return Map格式
   */
  def get(rowkey:String):Map[String,String] = {
    val hget = new Get(Bytes.toBytes(rowkey))
    val result = table.get(hget)
    val ret = result.rawCells().map(rawKV =>
      Map(Bytes.toString(CellUtil.cloneFamily(rawKV)) + ":" + Bytes.toString(CellUtil.cloneQualifier(rawKV)) -> Bytes.toString(CellUtil.cloneValue(rawKV)))
    )
    if (ret.length >= 1) ret.reduce(_ ++ _) else Map()
  }

  /**
   * 根据rowkey List获取相应的记录
   * param rowkeys
   * return
   */
  def get(rowkeys:List[String]):Array[(String,Map[String,Array[Byte]])] = {
   // var ret:Array[(String,Map[String,String])] = Array()
    val getList = rowkeys.map(x => new Get(Bytes.toBytes(x)))
    val result = table.get(getList)
    val ret = result.filter(_.getRow != null).map(x => {
      val rmap = x.rawCells().map(rawKV =>
        Map(Bytes.toString(CellUtil.cloneFamily(rawKV)) + ":" + Bytes.toString(CellUtil.cloneQualifier(rawKV)) -> CellUtil.cloneValue(rawKV))
      )
      if (rmap.length >= 1) (Bytes.toString(x.getRow),rmap.reduce(_ ++ _)) else (Bytes.toString(x.getRow),Map[String, Array[Byte]]())
    })
    ret
  }


  /**
   * 根据rowkey区间和列区间scan hbase
   * param family eg. cf
   * param rowStart eg. 2015
   * param rowStop eg. 2016
   * param columnStart. cv522255
   * param columnStop. cv522266
   * return
   */
  def scan(family:String,rowStart:String,rowStop:String,columnStart:String,columnStop:String):Array[(String,Map[String,String])] = {
    var ret:Array[(String,Map[String,String])] = Array()
    val s = new Scan()
    //设置rowkey范围
    s.setStartRow(Bytes.toBytesBinary(rowStart))
    s.setStopRow(Bytes.toBytesBinary(rowStop))

    //设置列的filter
    val filterList =  new FilterList(FilterList.Operator.MUST_PASS_ALL)
    filterList.addFilter(new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(family))))
    filterList.addFilter(new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(columnStart))))
    filterList.addFilter(new QualifierFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(columnStop))))
    s.setFilter(filterList)
    ret = scanAndConvert(s)
    ret
  }

  /**
   * 根据scan对象获取数据
   * param scan
   * return
   */
  def scanAndConvert(scan:Scan):Array[(String,Map[String,String])] = {
    var ret: Array[(String, Map[String, String])] = Array()
    val ss = table.getScanner(scan)
    val resultSet = ss.iterator()
    while (resultSet.hasNext) {
      val res = resultSet.next()
      val resultMap = mutable.Map[String, String]()
      res.rawCells().foreach(rawKV => {
        //获取列簇，列名，列值
        resultMap += (Bytes.toString(CellUtil.cloneFamily(rawKV)) + ":" + Bytes.toString(CellUtil.cloneQualifier(rawKV)) -> Bytes.toString(CellUtil.cloneValue(rawKV)))
      })
      ret = ret ++ Array((Bytes.toString(res.getRow), resultMap.toMap)) //结果收集
    }
    ret
  }

  /**
   * 单个记录删除
   * param rowkey
   */
  def delete(rowkey:String): Unit ={
    val delete = new Delete(Bytes.toBytes(rowkey))
    table.delete(delete)
  }

  def delete(rowkey:String,family:String,column:String): Unit ={
    val delete = new Delete(Bytes.toBytes(rowkey))
    delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column))
    table.delete(delete)
  }

  /**
   * 批量删除
   * param rowkeys
   */
  def delete(rowkeys:List[String]): Unit ={
    val deletes = rowkeys.map(rowkey => {
      new Delete(Bytes.toBytes(rowkey))
    })
    val deleteData = new util.ArrayList[Delete]()
    deletes.foreach(x => {
      deleteData.add(x)
    })
    table.delete(deleteData)
  }

  /**
   * 根据rowkeys和columns批量删除所有版本的数据
   * param rowkeys
   * param columns
   */
  def delete(rowkeys:List[String],columns:List[(String,String)]): Unit ={
    val deletes = rowkeys.map(rowkey => {
      val delete = new Delete(Bytes.toBytes(rowkey))
      columns.foreach(x => delete.addColumn(Bytes.toBytes(x._1),Bytes.toBytes(x._2)))
      delete
    })

    val deleteData = new util.ArrayList[Delete]()
    deletes.foreach(x => {
      deleteData.add(x)
    })
    table.delete(deleteData)
  }

}
