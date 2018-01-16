package com.hust;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class CtContent {
    private String title;
    private String content;
    private String ct_audit_state;


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }


    public String getContent() {
        return content;
    }


    public void setContent(String content) {
        this.content = content;
    }


    public String getCt_audit_state() {
        return ct_audit_state;
    }


    public void setCt_audit_state(String ct_audit_state) {
        this.ct_audit_state = ct_audit_state;
    }


    public static List<String> result1 = new ArrayList<String>();
    public static List<String> result2 = new ArrayList<String>();
    public static List<String> result3 = new ArrayList<String>();
    public static List<String> result4 = new ArrayList<String>();

    public static void main(String[] args) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        String text = FileUtils.readFileToString(new File("E:\\data.json"));
        Gson gson = new Gson();
        List<CtContent> retList = gson.fromJson(text,
                new TypeToken<List<CtContent>>() {
                }.getType());

        /**
         * 1. jsoup获取标题、正文标签
         * 2. jni对标题、正文分词
         * 3. 停用词处理 /user/iflyrd/falco/profile/ItemProfile/Resource/StopWords
         * 4. 计算tf-idf
         * 5. svm分类
         */

        for (int i = 0; i < retList.size(); i++) {
            Document doc = Jsoup.parseBodyFragment(retList.get(i).content);
            String ct = doc.select("p:not(img)").text();
            List<Term> termList = NLPTokenizer.segment(ct);
            for (Term term : termList) {
                String val = term.word;
                String nat = term.nature.toString();
                int off = term.offset;
                System.out.println(val + " nat: " + nat);
            }
            System.out.println("************************************************");
        }

        //需要计算准确率和召回率
        // 1、计算原本是审核通过，倍判断审核通过的总数
        //2、计算原本审核通过的，被判断为审核不同的概率

        //3、计算原本审核不通过的，被判断为审核不通过总数
        //4、计算原本审核不通过的，被判断为审核通过的概率
    }

}
