package cn.angetech.accumulatorTest;

import org.apache.spark.util.AccumulatorV2;
import scala.Int;

import java.util.HashMap;
import java.util.Map;

// String:进入类型  String:退出类型
public class UserDefinedAccumulator extends AccumulatorV2<String,String> {
    private String one = "ONE";
    private String two = "TWO";
    private String three = "THREE";
    private String delimit = "_";
    private String delimit2 = ":";
    private String data = one+delimit2+"0"+delimit+two+delimit2+"0"+delimit+three+delimit2+"0"+delimit;  // ONE:0_TWO:0_THREE:0
    private String zero = data;


    @Override
    public boolean isZero() {
        return data.equals(zero);
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        return new UserDefinedAccumulator();
    }

    @Override
    public void reset() {
        data = zero;
    }

    // v:  ONE:10  作为主入口，传入String 格式为： ONE:10
    @Override
    public void add(String v) {
        data = mergeData(v,data);
    }

    // 内部聚合accumulator的方法， 传入value，与本身的data聚合
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        data = mergeData(other.value(),data);
    }

    @Override
    public String value() {
        return data;
    }


    private String mergeData(String data1,String data2){
        // data1 新数据 ONE:0          data2 原数据 ONE:0_TWO:0_THREE:0  返回data2拼接后的类型
        StringBuffer res = new StringBuffer();
        String[] info1 = data1.split(delimit);  //[ONE:0 , ONE:0  ,]
        String[] info2 = data2.split(delimit);

        Map<String,Integer> map1 = new HashMap<>();
        Map<String,Integer> map2 = new HashMap<>();

        for(String info:info1){
            String[] kv = info.split(delimit2);
            if(kv.length == 2){
                String k = kv[0].toUpperCase();
                Integer v = Integer.valueOf(kv[1]);
                map1.put(k,v);
            }
        }

        for(String info:info2){
            String[] kv = info.split(delimit2);
            if(kv.length == 2){
                String k = kv[0].toUpperCase();
                Integer v = Integer.valueOf(kv[1]);
                map2.put(k,v);
            }
        }

        for(Map.Entry<String,Integer> entry:map1.entrySet()){
            String key = entry.getKey();
            Integer value = entry.getValue();
            if(map2.containsKey(key)){
                value = value + map2.get(key);
                map2.remove(key);
            }
            res.append(key+delimit2+value+delimit);
        }
        // 这个逻辑好像没用
//        for(Map.Entry<String, Integer> entry:map1.entrySet()){
//            String key = entry.getKey();
//            Integer value = entry.getValue();
//            if(res.toString().contains(key)){
//                continue;
//            }
//            res.append(key+delimit2+value+delimit);
//        }

        if(!map2.isEmpty()){
            for(Map.Entry<String,Integer> entry:map2.entrySet()){
                String key = entry.getKey();
                Integer value = entry.getValue();
                res.append(key+delimit2+value+delimit);
            }
        }
        return res.toString().substring(0,res.toString().length() -1);



    };
}
