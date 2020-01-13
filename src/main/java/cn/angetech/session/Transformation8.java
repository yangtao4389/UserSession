package cn.angetech.session;

import org.apache.commons.math3.analysis.integration.IterativeLegendreGaussIntegrator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Transformation8 {
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
                new Tuple2<Integer, Integer>(3,98),
                new Tuple2<Integer, Integer>(1,100),
                new Tuple2<Integer, Integer>(2,85),
                new Tuple2<Integer, Integer>(3,45)
        );

        // 并行化RDD
        JavaPairRDD<Integer,String> students = sparkContext.parallelizePairs(studentList);
        JavaPairRDD<Integer,Integer> scores = sparkContext.parallelizePairs(scoreList);

        // cogroup 聚合
        JavaPairRDD<Integer,Tuple2<Iterable<String>,Iterable<Integer>>> studentScores = students.cogroup(scores);

        studentScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println("==========");
                System.out.println("student id:"+t._1);
                System.out.println("student name:"+t._2._1);
                System.out.println("student score:"+t._2._2);

            }
        });



        sparkContext.close();
    }
}
