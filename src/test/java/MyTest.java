import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jjhu on 2017/2/16.
 */
public class MyTest extends TestCase {
    public void testRegex() {
//        Pattern p = Pattern.compile("^(\\d{1,4})(-|/)(\\d{1,2})\\2(\\d{1,2})$");
//        Matcher m = p.matcher("2017/02/122");
//        while (m.find()) System.out.println(m.group());
//
        String str = "ejjjjjengiiiiiiee";
        String regex = "(.)\\1+";
        String s = str.replaceAll(regex, "*");
        System.out.println(s);

//        String sta="123-;lang北斗狼神-,,,,,???？？？-";
//        String regex= "\\B";
//
//        String arrays[]=sta.split(regex);
//        for(String i:arrays){
//            System.out.println("["+i+"]");
//        }
    }

    public void testLinkedList(){
        String s1 = new String("hello");
        String s2 = new String("hello");
        if (s1 == s2) System.out.println("hello");
        if (s1.equals(s2)) System.out.println("world");

    }
}
