package com.hust

import java.io.{File, PrintWriter}
import java.util

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import org.apache.commons.io.FileUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jjhu on 2016/12/13.
  */
case class News(title:String, content:String, ct_audit_state:String)

object Segment {
    def main(args: Array[String]): Unit = {
        val text: String = FileUtils.readFileToString(new File("E:\\2016-12-13.json"))
        val gson: Gson = new Gson
        val retList :util.ArrayList[News] = gson.fromJson(text, new TypeToken[util.ArrayList[News]]() {}.getType())

        println(retList.size())

        /**
          * 1. jsoup获取标题、正文标签
          * 2. jni对标题、正文分词
          * 3. 停用词处理 /user/iflyrd/falco/profile/ItemProfile/Resource/StopWords
          * 4. 计算tf-idf
          * 5. svm分类
          */

        val writer = new PrintWriter(new File("E:\\data\\predict.txt"))

        for (i <- 0 until 200) {
            val doc: Document = Jsoup.parseBodyFragment(retList.get(i).content)
            val ct: String = doc.select("p:not(img)").text.replaceAll("""[\s~:]+""".r.toString(), "")
            val termList: java.util.List[Term] = NLPTokenizer.segment(ct)
            val title = retList.get(i).title.replaceAll("""[\s~:]+""".r.toString(), "")
            val titleList = NLPTokenizer.segment(title)

            if ((termList.size() + titleList.size()) > 0) {
                val item = new ArrayBuffer[String]()
                for (j <- 0 until titleList.size()) {
                    val v: String = titleList.get(j).word
                    val nat: String = titleList.get(j).nature.toString
                    item += Seq(v, nat).mkString(":")
                }
                for (j <- 0 until termList.size()) {
                    val v: String = termList.get(j).word
                    val nat: String = termList.get(j).nature.toString
                    item += Seq(v, nat).mkString(":")
                }

                var label = 0
                if (retList.get(i).ct_audit_state.equals("3")) {
                    label = 1
                }

                writer.write(Seq(Seq(title, label).mkString("|"), item.mkString("~")).mkString(" "))
                writer.write("\n")
                writer.flush()
            }
        }
        writer.close()
    }
}
