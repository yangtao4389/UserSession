package cn.angetech.accumulatorTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

public class CustomerAccumulator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("aaa");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("ONE","TWO","three","four","one"));
        UserDefinedAccumulator count = new UserDefinedAccumulator();
        sc.sc().register(count,"user_count");

        JavaPairRDD<String,String> pairRDD = rdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                int num = new Random().nextInt(10);
                return new Tuple2<>(s,s+":"+num);
            }
        });

        pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                count.add(stringStringTuple2._2);
                System.out.println(stringStringTuple2._2);
            }
        });
        System.out.println("the value of accumulator is:"+count.value());
    }
}
