package com.test

/**
 * Created by jjhu on 2016/5/20.
 */
object TopK {
  def main(args: Array[String]) {

  }

  def testHeapSort()={
    for (circle <- 0 until 20){
      val simArr:Array[(String,Double)] = Array.fill(1000000)(("1",math.random))
      for (i <- 0 until simArr.length){
        simArr(i) = (i.toString,simArr(i)._2)
      }

      val simArr1 = simArr
      val simArr2 = simArr

      //堆排测试
      val t1 = System.currentTimeMillis()
      heapSort(simArr1,0,20)
      val t2 = System.currentTimeMillis()

      //全量排序测试
      val t3 = System.currentTimeMillis()
      simArr2.sortWith(_._2 > _._2)
        .slice(0,20)
        .map{
        case (uid,score) => uid
      }.mkString("~")
      val t4 = System.currentTimeMillis()

      //比较效率
      println("HeapSort:" + (t2 - t1))
      println("NormalSort:" + (t4 - t3))
    }
  }

  def heapSort(simArr:Array[(String,Double)],
                     root:Int,
                     len:Int):String={

    val minHeap:Array[(String,Double)] = new Array[(String,Double)](len)
    for (i <- 0 until len){
      minHeap(i) = ("0",Double.MinValue)
    }

    for (item <- simArr){
      if (item._2 > minHeap(0)._2){
        minHeap(0) = item
        adjustHeap(minHeap,0,len)
      }
    }

    minHeap.map{
      case (uid,sim) => uid
    }.mkString("~")
  }

  def adjustHeap(heap:Array[(String,Double)],
                  root:Int,
                  len:Int):Unit={
    val left = 2*root + 1   //左孩子下标
    val right = 2*root + 2  //右孩子下标
    var min = root

    if (left < len && heap(left)._2 < heap(min)._2){
      min = left
    }

    if (right < len && heap(right)._2 < heap(min)._2){
      min = right
    }

    if (min != root){
      swap(heap,root,min)
      adjustHeap(heap,min,len)
    }
  }

  def swap(heap:Array[(String,Double)],
            i:Int,
            j:Int):Unit={
    val tmp = heap(i)
    heap(i) = heap(j)
    heap(j) = tmp
  }
}
