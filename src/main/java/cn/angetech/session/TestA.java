package cn.angetech.session;

import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class TestA {
    public static void main(String[] args) {
//        Iterable rows = Arrays.asList("2020-01-08","33","ca3c94bfe4754580be0bf6468a9759a0","1","click","2020-01-08 19:16:44",null,"9","28",null,null,null,null);
//        for(Object row:rows){
//            row.getS
//        }
        SparkSession ss = SparkSession.builder().appName("java spark sql basic example").config("spark.some.config.option","some value").master("local").getOrCreate();
        List<Integer> data = Arrays.asList(1,3,4,3,5);
//        JavaRDD<Integer> distData = ss.

        //JavaRDD<String> line1 = ss.sparkContext().parallelize(Arrays.asList("1 aa", "2 bb", "4 cc", "3 dd"));
//        SparkConf conf = new SparkConf().setAppName("dd").setMaster("local[2]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<Integer> distData = sc.parallelize(data);
//        SparkSession ss = new SparkSession(conf)
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<Integer> distData = jsc.parallelize(data);


//        val jsc = new JavaSparkContext(spark.sparkContext)



    }
}
