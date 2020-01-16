package cn.angetech.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static ThreadLocal<SimpleDateFormat> simpleDateFormatThreadLocal =  new ThreadLocal<SimpleDateFormat>(){
       protected SimpleDateFormat initialValue(){
           return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
       }
    };

    public static String getTodayDate(){
        return DATE_FORMAT.format(new Date());
    }
    public static String getLocalTime(){
        return TIME_FORMAT.format(new Date());
    }

    public static boolean before(String time1, String time2){
        try{
            Date dataTime1 =  TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);
            if(dataTime1.before(dateTime2)){
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public static boolean after(String time1, String time2) {
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);

            if(dateTime1.after(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // 时间差，秒
    public static int minus(String time1, String time2){
        try{
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);
            long millisecond = dateTime1.getTime() - dateTime2.getTime();
            return Integer.valueOf(String.valueOf(millisecond/1000));
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    //获取年月日和小时
    public static String getDateHour(String datetime){
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;  //  2020-01-10_15
    }

    // 获取昨天时间
    public static String getYesterdayDate(){
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -1);
        Date date = cal.getTime();
        return DATE_FORMAT.format(date);
    }

    // 格式化时间
    public static String formatDate(Date date){
        return DATE_FORMAT.format(date);
    }
    public static String formatTime(Date date){
        return TIME_FORMAT.format(date);
    }

    public static Date parseTime(String time){
        try{
            Date result = simpleDateFormatThreadLocal.get().parse(time);
            simpleDateFormatThreadLocal.remove();
            return result;
        }catch (ParseException e){
            e.printStackTrace();
        }
        return null;
    }




    public static void main(String[] args) {
//        System.out.println(getDateHour("2020-01-10 15:03:17"));
        System.out.println(getLocalTime());

    }



}
