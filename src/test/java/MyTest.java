import junit.framework.TestCase;

import java.util.*;

/**
 * Created by jjhu on 2017/2/16.
 */
public class MyTest extends TestCase {
    public void testRegex() {
//        Pattern p = Pattern.compile("^(\\d{1,4})(-|/)(\\d{1,2})\\2(\\d{1,2})$");
//        Matcher m = p.matcher("2017/02/122");
//        while (m.find()) System.out.println(m.group());
//
        Map map = new Hashtable<Integer, Integer>();
        map.put(1, null);
    }

    public void testLinkedList(){
        String s1 = new String("hello");
        String s2 = new String("hello");
        if (s1 == s2) System.out.println("hello");
        if (s1.equals(s2)) System.out.println("world");

    }
}
