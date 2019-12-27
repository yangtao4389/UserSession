package cn.angetech.conf;

import java.io.InputStream;
import java.util.Properties;

/*
 * 配置文件管理类
 *
 * */
public class ConfigurationManager {
    private static Properties prop = new Properties();

    /**
     * 通过静态代码块加载配置文件
     */
    static {
        try {
            InputStream is = ConfigurationManager.class.getClassLoader().getResourceAsStream("conf.properties");
            prop.load(is);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            Integer result = Integer.valueOf(value);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔型
     *
     * @param key
     * @return
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        if ("false".equals(value)) {
            return false;
        }
        return true;
    }
}
