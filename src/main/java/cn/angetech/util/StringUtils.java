package cn.angetech.util;

public class StringUtils {

    public static String fulfuill(String str){
        if(str.length() == 2){
            return str;
        }else {
            return "0"+str;
        }
    }

}
