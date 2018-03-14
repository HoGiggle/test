package com.util;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.CustomDictionary;

/**
 * Created by jjhu on 2018/3/6.
 */
public class NlpDictionnaryUpdate {
    public static void main(String[] args)
    {
        // 动态增加
//        CustomDictionary.add("王者荣耀");
        // 强行插入
        CustomDictionary.insert("白富美", "nz 1024");
        // 删除词语（注释掉试试）
//        CustomDictionary.remove("王者荣耀");
        System.out.println(CustomDictionary.add("单身狗", "nz 1024 n 1"));
        System.out.println(CustomDictionary.get("单身狗"));

        String text = "王者荣耀攻城狮逆袭单身狗，迎娶白富美，走上人生巅峰";  // 怎么可能噗哈哈！

        // 标准分词
        System.out.println(HanLP.segment(text));
    }
}
