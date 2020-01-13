package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Array;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Transformation12 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);


        List<Tuple2<String,String>> scoreList = Arrays.asList(
                new Tuple2<String,String>("class1","leo"),
                new Tuple2<String,String>("class2","yy"),
                new Tuple2<String,String>("class1","dd"),
                new Tuple2<String,String>("class2","leeeo"),
                new Tuple2<String,String>("class2","tt")
        );

        // 并行化集合
        JavaPairRDD<String,String> students = sparkContext.parallelizePairs(scoreList);

        // countByKey  统计每个班级的学生人数，统计每个key对应的元素个数 返回Map类型
        Map<String,Long> studengtCounts = students.countByKey();

        for(Map.Entry<String,Long> studentCount:studengtCounts.entrySet()){
            System.out.println(studentCount.getKey()+":"+studentCount.getValue());
        }




        sparkContext.close();
    }
}
