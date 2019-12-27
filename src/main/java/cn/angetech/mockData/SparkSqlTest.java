package cn.angetech.mockData;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlTest {
    // starting point;sparkSession


    // create DataFrames
    private static void runBasicData(SparkSession spark2) throws Exception{
        SparkSession spark = SparkSession.builder().appName("java spark sql basic example").config("spark.some.config.option","some value").getOrCreate();
        Dataset<Row> df = spark.read().json("examples/src/main/resrouces/people.json");
        df.show();
    }


    public static void main(String[] args) throws Exception{
        SparkSession spark = SparkSession.builder().appName("java spark sql basic example").config("spark.some.config.option","some value").getOrCreate();
        runBasicData(spark);
        System.out.println("");
    }
}
