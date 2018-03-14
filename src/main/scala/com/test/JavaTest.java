package com.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by jjhu on 2016/12/23.
 */
public class JavaTest {
    public static void main(String[] args) {

////        Segment nShortSegment = new NShortSegment().enableCustomDictionary(true).enablePlaceRecognize(true).enableOrganizationRecognize(true);
////        Segment shortestSegment = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
//        Segment nlpSegment = HanLP.newSegment().enableCustomDictionary(true);
//        HanLP.Config.enableDebug();
//        String[] testCase = new String[]{
//                "英雄联盟和王者荣耀都是很好玩的游戏闪现一级团长的帅"
//        };
//        for (String sentence : testCase)
//        {
////            System.out.println("N-最短分词: " + nShortSegment.seg(sentence));
////            System.out.println("最短路分词: " + shortestSegment.seg(sentence));
//            System.out.println("NLP: " + nlpSegment.seg(sentence));
//        }

        readFileByLines("E:\\data\\wordsMapping.txt");
    }

    public static void readFileByLines(String fileName) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(fileName)));
            String line;
            int num = 10;
            while (((line = reader.readLine()) != null) && (num-- >= 0)) {
                System.out.println(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}
