package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Transformation9 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numbers = sparkContext.parallelize(numberList);

        // 使用reduce操作对集合中的数字进行累加
        int sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        System.out.println(sum);




        sparkContext.close();
    }
}
