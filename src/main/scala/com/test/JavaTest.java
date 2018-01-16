package com.test;

import com.hankcs.hanlp.seg.Dijkstra.DijkstraSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;

/**
 * Created by jjhu on 2016/12/23.
 */
public class JavaTest {
    public static void main(String[] args) {

        Segment nShortSegment = new NShortSegment().enableCustomDictionary(true).enablePlaceRecognize(true).enableOrganizationRecognize(true);
        Segment shortestSegment = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
        String[] testCase = new String[]{
                "英雄联盟和王者荣耀都是很好玩的游戏闪现一级团长的帅"
        };
        for (String sentence : testCase)
        {
            System.out.println("N-最短分词: " + nShortSegment.seg(sentence));
            System.out.println("最短路分词: " + shortestSegment.seg(sentence));
            System.out.println("NLP: " + NLPTokenizer.segment(sentence));
        }
    }
}
