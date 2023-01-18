package cn.itcast.hdfs;

import org.apache.log4j.Logger;

/**
 * @description:
 * @author: Allen Woon
 * @time: 2021/1/8 18:56
 */
public class Log4jTest {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(Log4jTest.class);
        logger.debug("这是debug");
        logger.info("这是info");
        logger.warn("这是warn");
        logger.error("这是error");
        logger.fatal("这是fatal");
    }
}
