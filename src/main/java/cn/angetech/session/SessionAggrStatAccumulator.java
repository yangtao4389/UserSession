package cn.angetech.session;


import cn.angetech.constant.Constants;
import cn.angetech.util.StringUtils;
import org.apache.spark.util.AccumulatorV2;

public class SessionAggrStatAccumulator extends AccumulatorV2<String,String>{
    private String str = zero();

    private String zero(){
        return  Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    @Override
    public boolean isZero() {
        return str==zero();
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        return new SessionAggrStatAccumulator();
    }

    @Override
    public void reset() {
        str = zero();
    }

    // 直接添加 Constants.TIME_PERIOD_7s_9s
    @Override
    public void add(String v) {
        String value = StringUtils.getFieldFromConcatString(str,"\\|",v);
        if(value != null){
            int newValue = Integer.valueOf(value) + 1;
            str = StringUtils.setFieldInConcatString(str,"\\|",v,String.valueOf(newValue));
        }
    }

    // 这里出错 融合两个 session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    @Override
    public void merge(AccumulatorV2<String, String> other) {
//        str = other.value();
//        System.out.println(str+"ssssssssssssssssssssssss");
//        System.out.println(other.value()+"ooooooooooooooooooo");
        String delimiter = "\\|";
        String[] fields1 = str.split(delimiter);
        String[] fields2 = other.value().split(delimiter);
        for(int i=0;i<fields1.length;i++){
            String filename1 = fields1[i].split("=")[0];
            Long value1 = Long.valueOf(fields1[i].split("=")[1]);
            for(int j=0;j<fields2.length;j++){
               String filename2 = fields2[j].split("=")[0];
               if(filename1.equals(filename2)){
                   Long value2 = Long.valueOf(fields2[i].split("=")[1]);
                   Long newValue = value1+value2;
                   str = StringUtils.setFieldInConcatString(str,delimiter,filename2,String.valueOf(newValue));
               }
            }
        }



    }

    @Override
    public String value() {
        return str;
    }


}
