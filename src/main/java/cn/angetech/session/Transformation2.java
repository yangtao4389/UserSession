package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;

import java.util.Arrays;
import java.util.List;

public class Transformation2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,32,34,5,4,7);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);

        // 过滤其中的偶数
        JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer%2 == 0;
            }

        });

        // 打印
        evenNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });





        sparkContext.close();
    }
}
