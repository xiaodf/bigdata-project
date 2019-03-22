package com.bigdata.hbase.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 读取Properties综合类
 */
public class PropertiesUtil {

    private static String filePath = "application.properties";
//    private static String filePath = "classpath:application.properties";

    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class.getName());

    private static Properties properties = new Properties();

    /**
     * 保存配置文件中的键和值
     */
    private static Map<String, Object> map = new HashMap<String, Object>();

    /**
     * 获取str类型的值
     *
     * @param key properties键值
     * @param def 默认值
     * @return
     */
    public static String getValueStr(String key, String def) {
        Object val = map.get(key);
        return null == val ? def : val.toString();
    }

    /**
     * 读取int类型的数字
     *
     * @param key properties键值
     * @param def 默认值
     * @return
     */
    public static Integer getValueInt(String key, int def) {
        Object val = map.get(key);
        int reVal = def;
        try {
            reVal = null == val ? def : Integer.parseInt(val.toString());
        } catch (Exception e) {
            logger.info("数据类型转换有误，请检查配置文件中键[" + key + "]值的类型！");
        }
        return reVal;
    }

    static {
        try {
            properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(filePath));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static String getProperty(String property) {
        return (String) properties.get(property);
    }

    public static void main(String[] args) {
        String property = PropertiesUtil.getProperty("anticheat.mobile.shumei.url");
        System.out.println(property);
    }
}