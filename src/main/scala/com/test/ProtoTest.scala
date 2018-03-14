package com.test

import com.util.CommonService
import junit.framework.TestCase

/**
  * Created by jjhu on 2017/2/14.
  */
object ProtoTest extends TestCase{
  def testSourceCode(): Unit ={
    val sc = CommonService.scLocalInit(this.getClass.getName)
    val rdd1 = sc.textFile("").map((_, 1))
    rdd1.groupByKey()
  }
}
