package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Transformation5 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 构造集合
        List<Tuple2<String,Integer>> scoreList = Arrays.asList(
                new Tuple2<String ,Integer>("class1",80),
                new Tuple2<String ,Integer>("class2",75),
                new Tuple2<String ,Integer>("class1",90),
                new Tuple2<String ,Integer>("class2",65)
        );

        // 并行化集合
        JavaPairRDD<String,Integer> scores = sparkContext.parallelizePairs(scoreList);

        // reduceByKey 算子
        // new Function2  三个值： 第一个：value，第二个：value  第三个：对value返回的类型，默认跟value一致
        JavaPairRDD<String,Integer> totalScores = scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }) ;


        totalScores.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+"："+stringIntegerTuple2._2);
            }
        });




        sparkContext.close();
    }
}
