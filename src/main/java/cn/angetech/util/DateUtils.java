package cn.angetech.util;


import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public static String getTodayDate(){
        return DATE_FORMAT.format(new Date());
    }

}
