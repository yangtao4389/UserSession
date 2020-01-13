package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Transformation7 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1,"leo"),
                new Tuple2<Integer, String>(2,"jack"),
                new Tuple2<Integer, String>(3,"tom")
        );

        List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1,100),
                new Tuple2<Integer, Integer>(2,90),
                new Tuple2<Integer, Integer>(3,98)
        );

        // 并行化两个RDD
        JavaPairRDD<Integer,String> students = sparkContext.parallelizePairs(studentList);
        JavaPairRDD<Integer,Integer> scores = sparkContext.parallelizePairs(scoreList);

        // join
        JavaPairRDD<Integer,Tuple2<String,Integer>> studentScores = students.join(scores);

        // 打印
        studentScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.println("==================");
                System.out.println("class:"+integerTuple2Tuple2._1);
                System.out.println("name:"+integerTuple2Tuple2._2._1);
                System.out.println("score:"+integerTuple2Tuple2._2._2);
            }
        });




        sparkContext.close();
    }
}
