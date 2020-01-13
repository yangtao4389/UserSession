package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Transformation6 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 构造集合
       List<Tuple2<Integer,String>> scoreList = Arrays.asList(
               new Tuple2<Integer, String>(65,"leo"),
               new Tuple2<Integer, String>(89,"tom"),
               new Tuple2<Integer, String>(80,"marr"),
               new Tuple2<Integer, String>(50,"jack")
       );

       JavaPairRDD<Integer,String> scores = sparkContext.parallelizePairs(scoreList);

       // 只改变顺序
       JavaPairRDD<Integer,String> sortedScores = scores.sortByKey();  // false 降序，true 升序 默认为true

        sortedScores.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._1+":"+integerStringTuple2._2);
            }
        });



        sparkContext.close();
    }
}
