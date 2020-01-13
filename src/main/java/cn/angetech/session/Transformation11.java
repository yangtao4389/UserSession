package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Transformation11 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);

        // 对rdd使用count操作，统计它有多少个元素
        long count = numbers.count();
        System.out.println(count);


        // take 获取前n个数据
        List<Integer> top3Numbers = numbers.take(3);
        for(Integer num:top3Numbers){
            System.out.println(num);
        }




        sparkContext.close();
    }
}
