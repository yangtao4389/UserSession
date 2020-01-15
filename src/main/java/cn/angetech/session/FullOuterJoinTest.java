package cn.angetech.session;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.Optional;
import java.util.Random;

// 参考：https://www.jianshu.com/p/7e4c83f1c24f
public class FullOuterJoinTest {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("tttttt").config("spark.some.config.option","some value").master("local").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());  //
        List<Integer> data = Arrays.asList(1,2,3,4,5,6,7);
        final Random random = new Random();
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,random.nextInt(10));
            }
        });

        // 全关联
        JavaPairRDD<Integer,Tuple2<Optional<Integer>,Optional<Integer>>> fullJoinRDD = javaPairRDD.fullOuterJoin(javaPairRDD);
    }
}
