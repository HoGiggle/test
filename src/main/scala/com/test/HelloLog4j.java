package com.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by jjhu on 2017/1/9.
 */
public class HelloLog4j {
    private static Logger logger = Logger.getLogger(HelloLog4j.class);
//    private static Log logger = LogFactory.getLog(HelloLog4j.class);

    public static void main(String[] args){
        // System.out.println("This is println message.");

//        BasicConfigurator.configure();
        PropertyConfigurator.configure("resources/log4j.properties");
        // 记录debug级别的信息
        logger.debug("This is debug message.");
        // 记录info级别的信息
        logger.info("This is info message.");
        // 记录error级别的信息
        logger.error("This is error message.");
    }
}
