package cn.angetech.accumulatorTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class myStringAccu {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("aaa");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("A","B","C","D","E","F","G","H","I");
        final JavaRDD<String> javaRDD = sc.parallelize(list,3).cache();

        final StrAccu sa =new StrAccu();
        sc.sc().register(sa,"sa");
        javaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                sa.add(s);
            }
        });
        System.out.println(sa.value());  //A_B_C_D_E_F_G_H_I_
    }
}
