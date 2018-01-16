package com.surus

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jjhu on 2017/2/24.
  */
object Utils {
    def dateRange2StrList(startDateStr: String,
                          endDateStr: String): Array[String] = {
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val calendar = Calendar.getInstance()
        calendar.setTime(sdf.parse(startDateStr))

        val result: ArrayBuffer[String] = ArrayBuffer()
        val endDate = sdf.parse(endDateStr)
        while (calendar.getTime.before(endDate)){
            result += sdf.format(calendar.getTime)
            calendar.add(Calendar.DAY_OF_YEAR, 1)
        }
        result += endDateStr

        result.toArray
    }
}
