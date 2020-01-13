package cn.angetech.util;

import cn.angetech.constant.Constants;

public class StringUtils {

    public static boolean isEmpty(String str){return str == null || "".equals(str);}

//    public static boolean isNotEmpty(String str){return str != null && !"".equals(str);}
    public static boolean isNotEmpty(String str){return !isEmpty(str);}

    public static String trimComma(String str){
        if(str.startsWith(",")){
            str = str.substring(1);
        }
        if(str.endsWith(",")){
            str = str.substring(0,str.length()-1);
        }
        return str;
    }

    // 补全两位数字
    public static String fulfuill(String str){
        if(str.length() == 2){
            return str;
        }else {
            return "0"+str;
        }
    }


    // 提取值，  aa=1&bb=1  这种提取吧。
    public static String getFieldFromConcatString(String str, String delimiter, String field){
        String[] fields = str.split(delimiter);
        for(String concatField:fields){
            if(concatField.split("=").length == 2){
                String fieldName = concatField.split("=")[0];
                String fieldValue = concatField.split("=")[1];
                if(fieldName.equals(field)){
                    return fieldValue;
                }
            }
        }
        return null;
    }

    public static String setFieldInConcatString(String origalStr, String delimiter, String field, String newFieldValue){
        String[] fields = origalStr.split(delimiter);
        for (int i=0;i<fields.length;i++){
            String fieldName = fields[i].split("=")[0];
            if(fieldName.equals(field)){
                String concatField = fieldName + "="+newFieldValue;
                fields[i] = concatField;
                break;
            }
        }
        StringBuffer buffer = new StringBuffer("");
        for (int i=0;i<fields.length;i++){
            buffer.append(fields[i]);
            if(i<fields.length-1){
                buffer.append("|");
            }
        }

        return buffer.toString();
    }

    public static void main(String[] args) {
        String sessionPartInfo = "sessionId=bf2f62bab0fa46ab80f8cca29d2a0b4d|searchKeywords=|clickCategoryIds=23,24|visitLength=2653|stepLength=6|startTime=2020-01-10 16:12:48";
        String session = StringUtils.getFieldFromConcatString(sessionPartInfo,"\\|", Constants.FIELD_SESSIONID);
        System.out.println(session);
        String ss = StringUtils.setFieldInConcatString(sessionPartInfo,"\\|",Constants.FIELD_SESSIONID,"ddddddddddd");
        System.out.println(ss);

//        String s = "session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0";
//        String[] fields = s.split("\\|");
//        System.out.println(fields[0]);
//        for(int i=0;i<fields.length;i++){
//            String filename1 = fields[i].split("=")[0];
//            Long value1 = Long.valueOf(fields[i].split("=")[1]);
//            System.out.println(filename1);
//            System.out.println(value1);
//        }

    }

}
