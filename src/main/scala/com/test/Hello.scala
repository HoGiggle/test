package com.test

import java.io.File
import java.text.{DecimalFormat, NumberFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.util.CommonService
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random
import scala.util.parsing.json.JSON

/**
  * Created by admin on 2016/4/20.
  */
class Mapper(n: Int) extends Serializable{
    @transient lazy val log = org.apache.log4j.LogManager.getLogger("uba")

    def doSomeMappingOnDataSetAndLogIt(rdd: RDD[Int]): RDD[String] =
        rdd.map{ i =>
            log.info("mapping: " + i)
            (i + n).toString
        }
}

object Hello {
    class ThreadDemo(threadName: String) extends Runnable {
        override def run() {
            try {
                println(threadName + "Start")
                throw new Exception
            } catch {
                case e: Exception => {

                }
            }

            for (i <- 1 to 3) {
                println(threadName + "|" + i)
            }
        }
    }

    //  val logger = Logger.getLogger(this.getClass)
    private val hexString: String = "0123456789ABCDEF"

    def main(args: Array[String]): Unit = {
//        val job = new Job()
//        job.setOutputFormatClass(classOf[TextOutputFormat[LongWritable,Text]])
//        job.getConfiguration().set("mapred.output.compress", "true")
//        job.getConfiguration().set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
//        val textFile = sc.newAPIHadoopFile(args(0), classOf[LzoTextInputFormat],classOf[LongWritable], classOf[Text],job.getConfiguration())
//        textFile.saveAsNewAPIHadoopFile(args(1), classOf[LongWritable], classOf[Text],classOf[TextOutputFormat[LongWritable,Text]],job.getConfiguration())

      val sdf = new SimpleDateFormat("yyyyMMdd")
      val date = sdf.parse("20170321")
      val cal = Calendar.getInstance()
      cal.setTime(date)
      cal.add(Calendar.WEEK_OF_MONTH, 1)
      println(sdf.format(cal.getTime))
    }

  def dateCreate(start: String, end: String): Array[String] = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val sDate = sdf.parse(start)
    val eDate = sdf.parse(end)

    val cal = Calendar.getInstance()
    cal.setTime(sDate)
    val endCal = Calendar.getInstance()
    endCal.setTime(eDate)
    val arr = new ArrayBuffer[String]()
    while(cal.before(endCal) || cal.equals(endCal)) {
      arr.append(sdf.format(cal.getTime))
      cal.add(Calendar.DAY_OF_YEAR, 1)
    }

    arr.toArray
  }

    def testContact() = {
        val s = Source.fromFile(new File("E:\\acdata.txt")).getLines()
        for (line <- s){
            val map = CommonService.MapLoader(line)
            val uid = map.getOrElse("uid", "")
            val contactInfo = map.getOrElse("flumedesds", "")

            import org.json4s.native.JsonMethods._
            val json = parse(contactInfo)
            val jsonMap = json.values.asInstanceOf[Map[String,_]]

            if (jsonMap.contains("contactInfo") && uid.length > 0){
//                println(uid + "**************************")
                //                val tmp = jsonMap("contactInfo") match {
                //                    case itemList:List[_] => {
                //
                //                    }
                //                }
                //                tmp

                for(item <- jsonMap.getOrElse("contactInfo", "").asInstanceOf[List[_]]){

                    for ((name, num) <- item.asInstanceOf[Map[String, Map[String, List[_]]]]){
                        val cleanName= name.trim.replaceAll("\\~", "")
                        for (sis <- num.getOrElse("sis", "").asInstanceOf[List[Map[String, _]]]){
                            try {
                                for (nm <- sis.getOrElse("nm", "").asInstanceOf[List[Map[String, _]]]){
                                    val tmp = nm.get("ifi")
                                    tmp match {
                                        case Some(str: String) => {
                                            if (!CommonService.isPhoneNum(CommonService.cleanPhoneNum(str))) {
                                                println(cleanName + " " + str)
                                            }
                                        }
                                    }
                                }
                            } catch {
                                case e:Exception => {
//                                    e.printStackTrace()
//                                    println(sis)
                                }
                            }

                        }
                    }
                    //Map(孝雄 -> Map(sis -> List(Map(nm -> List(Map(ifi -> 13217325690, ift -> Map(cti -> 2)))))))
                }
            }
        }
    }

    object Context {
        implicit val ccc: String = "nihao"
    }


    object Param {
        def print(content: String)(implicit prefix: String) {
            println(prefix + ":" + content)
        }
    }

    def getDateRange(pastDays:Int):String = {
        val df = new SimpleDateFormat("yyyyMMdd")
        val cal = Calendar.getInstance()
        var result = Seq[String]()

        for (i <- 1 to pastDays){
            cal.add(Calendar.DATE, -1)
            result = result :+ df.format(cal.getTime)
        }
        val range = result.mkString(",")
        s"{$range}"
    }



    def segment(s:String) = {
        val seg = NLPTokenizer.segment(s)
        for (i <- 0 until seg.size()){
            val item = seg.get(i)
            println(item.word + ":" + item.nature.toString)
        }
    }

    def randomNew(n:Int, len:Int):List[Int] = {
        var resultList:List[Int] = Nil
        while(resultList.length < n){
            val randomNum = (new Random).nextInt(len)
            if(!resultList.contains(randomNum)){
                resultList = resultList:::List(randomNum)
            }
        }
        resultList
    }

    def testTryCatch(): String = {
        var t = ""
        try {
            t = "try"
            return t
        } catch {
            case e: Exception => {
                t = "catch"
                return t
            }
        } finally {
            t = "finally"
        }
    }

    def testImplicit(): Unit = {
        implicit val a = "yang"
        Param.print("jack")("hello")

        //    import Context._
        Param.print("jack")
    }

    def getFormatDate(dateStr: String): String = {
        val df = new SimpleDateFormat("yyyyMMdd")
        df.formatted(dateStr)
    }

    /**
      * 解析App配置Json信息
      *
      * @param str json string
      * @return pacName~appName~cateName~tags
      */
    def parseJson(str: String): String = {
        val json = JSON.parseFull(str)
        val result = json match {
            case Some(m: Map[String, Any]) => {
                val pName = m("pname") match {
                    case s: String => s
                }

                val cateName = m("cate_name") match {
                    case s: String => s
                }

                val tags = m("tags") match {
                    case s: List[String] => s.mkString(":")
                }

                val appName = m("title") match {
                    case s: String => s
                }

                if ((pName.length > 0) && ((tags.length > 0) || (cateName.length > 0))) {
                    Seq(pName, appName, cateName, tags).mkString("~")
                } else {
                    null
                }
            }
        }

        result
    }

    def hex2Str(hex: String): String = {
        val strArr = hex.split("\\\\")
        val byteArr: Array[Byte] = new Array[Byte](strArr.length)
        for (i <- 1 until byteArr.length) {
            val hexInt = Integer.decode("0" + strArr(i))
            byteArr(i - 1) = hexInt.byteValue()
        }
        new String(byteArr, "UTF-8")
    }

    def encodeHex(str: String): String = {
        val bytes = str.getBytes();
        val sb = new StringBuilder(bytes.length * 2);
        for (i <- 0 to bytes.length - 1) {
            sb.append(hexString.charAt((bytes(i) & 0xf0) >> 4));
            sb.append(hexString.charAt(bytes(i) & 0x0f));
        }
        return sb.toString();
    }

    def exceptionTest(): Unit = {
        val tmp = 0
        try {
            val i = 1 / tmp
        } catch {
            case e: Exception => {
                var loopTimes = 0
                var isSucceed = false

                println(s"storeIntoCordis: try to connect redis $loopTimes times.")
                while ((!isSucceed) && (loopTimes < 5)) {
                    Thread.sleep(5)
                    try {
                        val j = 1 / tmp
                        isSucceed = true
                    } catch {
                        case e: Exception => {
                            loopTimes += 1
                            println(s"storeIntoCordis: try to connect redis $loopTimes times.")
                            e.printStackTrace()
                        }
                    }
                }
                //        logger.info("While over:" + isSucceed)
                if (!isSucceed) {
                    println(s"########################### try to connect redis $loopTimes times. ##########################")
                    e.printStackTrace()
                }
            }
        }
    }

    def getTimeStamp(today: String): String = {
        val df = new SimpleDateFormat("yyyyMMdd")
        df.parse(today).getTime.toString
    }

    def testTupltSet() = {
        val set = Iterable(("display", "0"), ("display", "1"), ("click", "0"), ("click", "1"))
        val set1 = Iterable(("click", "0"), ("click", "1"), ("display", "1"))
        if (set.toSet.contains(("display", "0")) || set.toSet.contains(("display", "1"))) {
            println("contains1")
        }

        if (set1.toSet.contains(("display", "0")) || set1.toSet.contains(("display", "1"))) {
            println("contains2")
        }
    }

    def getOtherDate(today: Date, df: SimpleDateFormat, distance: Int): String = {
        val calendar = Calendar.getInstance() //得到日历
        calendar.setTime(today); //把当前时间赋给日历
        calendar.add(Calendar.DAY_OF_MONTH, distance)
        df.format(calendar.getTime)
    }

    def getOtherDate(date: String, distance: Int): String = {
        val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val today = df.parse(date)
        val calendar = Calendar.getInstance() //得到日历
        calendar.setTime(today); //把当前时间赋给日历
        calendar.add(Calendar.DAY_OF_MONTH, distance)
        df.format(calendar.getTime)
    }

    def setDate() = {
        val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val today = df.parse("20160515")
        val calendar = Calendar.getInstance() //得到日历
        calendar.setTime(today); //把当前时间赋给日历
        calendar.add(Calendar.DAY_OF_MONTH, -1) //设置为前一天
        val outDay = calendar.getTime() //得到前一天的时间
    }

    def doubleFormat(): Unit = {
        val nFormat = NumberFormat.getNumberInstance()
        nFormat.setMaximumFractionDigits(4) //设置小数点后面位数为
        System.out.println(nFormat.format(14.71556d))

        println((1 * 1.0 / 30000000).toString)
        println((1 * 1.0 / 30000000).toString.substring(0, 6))

        val d = 14.7155d;
        val df0 = new DecimalFormat("###")
        val df1 = new DecimalFormat("###.0")
        val df2 = new DecimalFormat("###.00")
        println(df0.format(d))
        println(df1.format(d))
        println(df2.format(d))
    }

    def distancsOfDay(start: String, end: String): Long = {
        val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
        val sDate = df.parse(start)
        val eDate = df.parse(end)
        val days = (eDate.getTime - sDate.getTime) / (1000l * 3600 * 24)

        days
    }

    def getMinuteDistance(sTime: String, eTime: String): Long = {
        val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
        val start = df.parse(getDate(sTime))
        val end = df.parse(getDate(eTime))
        val res = ((end.getTime - start.getTime) / 60000)

        if (res <= 29) {
            res
        } else {
            29 //30分钟以外记为30分钟
        }
    }

    def getDate(time: String): String = {
        if (time.isEmpty || time.length == 0) {
            time
        }
        else {
            time.replaceAll("[-:.\\s]", "").substring(0, 14)
        }
    }

    def pathTest(): Unit = {
        /*val list = List("20160506","20160507",
         "20160508","20160509",
         "20160510","20160511",
         "20160512","20160513",
         "20160514","20160527")*/

        val sc = new SparkContext("local", "hello", new SparkConf())
        val inPath = "/user/iflyrd/falco/UserActions/2016050[7-9]|2016051[0-3]/*"

        sc.textFile(inPath)
            .map(_.split("~"))
            .map {
                case arr => arr(0)
            }
            .distinct()
            .foreach(println)

        /* for(item <- list){
           if (item.matches("2016051[0-3]\\|2016050[7-9]"))
             println(item)

         }*/
    }
}
