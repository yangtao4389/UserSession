package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import scala.Int;

import java.util.Arrays;
import java.util.List;

public class Transformation1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,32,34,5,4,7);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);

        // 将集合中的元素都*2
        JavaRDD<Integer> doubleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer*2;
            }
        });

        // 打印新的RDD
        doubleNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sparkContext.close();
    }
}
