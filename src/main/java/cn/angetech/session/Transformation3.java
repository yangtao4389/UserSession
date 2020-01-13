package cn.angetech.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Transformation3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 构造集合
        List<String> lineList = Arrays.asList("hello you","hello me","hello world");
        JavaRDD<String> lines = sparkContext.parallelize(lineList);

        // 将每一行文本，拆分为多个单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();  // 必须返回iterable类型
            }
        });

        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });






        sparkContext.close();
    }
}
