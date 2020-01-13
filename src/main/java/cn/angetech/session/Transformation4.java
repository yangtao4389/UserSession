package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Array;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Transformation4 {
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

        // groupByKey 算子，返回的是JavaPairRDD，只是泛型后面变为Iterable，也就是安装key进行集合，value聚合成Iterable
        JavaPairRDD<String,Iterable<Integer>> groupedScores = scores.groupByKey();

        // 打印
        groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> s) throws Exception {
                System.out.println("======================");
                System.out.println(s._1);  // key
                Iterator<Integer> ite = s._2.iterator();
                while (ite.hasNext()){
                    System.out.println(ite.next());
                }
            }
        });





        sparkContext.close();
    }
}
