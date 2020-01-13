package cn.angetech.session;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.janino.Java;

import java.io.Serializable;
import java.util.List;
// 参考http://smartsi.club/spark-base-how-to-use-accumulator.html
public class CustomAccumulatorExample implements Serializable{
    public static void main(String[] args) {
        String appName = "CustomAccumulatorExample";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[3]");;
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<String> list = Lists.newArrayList();
        list.add("27.34832,111.32135");
        list.add("34.88478,185.17841");
        list.add("39.92378,119.50802");
        list.add("94,119.50802");

        CollectionAccumulator<String> collectionAccumulator = new CollectionAccumulator<>();
        sparkContext.sc().register(collectionAccumulator,"Illegal Coordinates");
        // 原始坐标
        JavaRDD<String> sourceRDD = sparkContext.parallelize(list);
        // 过滤非法坐标
        JavaRDD<String> resultRDD = sourceRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] coordinate = s.split(",");
                double lat = Double.parseDouble(coordinate[0]);
                double lon = Double.parseDouble(coordinate[1]);
                if(Math.abs(lat)>90 || Math.abs(lon)>180){
                    collectionAccumulator.add(s);
                    return true;
                }
                return false;
            }
        });

        // 输出
        resultRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String coordinate) throws Exception {
                System.out.println("[Data]"+coordinate);
            }
        });
        // 查看异常坐标
        for(String coordinate: collectionAccumulator.value()){
            System.out.println("[Illegal]:"+coordinate);
        }


    }
}
