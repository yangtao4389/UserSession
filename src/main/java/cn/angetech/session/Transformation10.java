package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.List;

public class Transformation10 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);

        // 使用map操作将集合中所有数字乘以2
        JavaRDD<Integer> doubleNumber = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer*2;
            }
        });

        // collect() 动作将所有元素拉到本地，建议用for来循环，否则会出错
        List<Integer> doubleNumberList = doubleNumber.collect();
        for(Integer num:doubleNumberList){
            System.out.println(num);
        }






        sparkContext.close();
    }
}
