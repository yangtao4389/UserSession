package cn.angetech.mockData;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class MockData2 {
    public static void mock(SparkSession sparkSession){
        List<Row> rowList = new MockSession().mock();
//        for (Row row:rowList){
//            System.out.println(row);
//            System.out.println(row.getString(9));
//
//        }
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action", DataTypes.StringType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)
        ));

        // Apply the schema to the RDD
        Dataset<Row> userVisitDataFrame = sparkSession.createDataFrame(rowList, schema);
        userVisitDataFrame.createOrReplaceTempView("user_visit_action");

        // todo 使用方法
//        Dataset<Row> results = sparkSession.sql("SELECT * FROM user_visit_action limit 10");
        Dataset<Row> results = sparkSession.sql("SELECT * FROM user_visit_action ");
        results.show();

        List<Row> rowListUser = new MockUser().mock();
        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)));

        Dataset<Row> userDataFrame = sparkSession.createDataFrame(rowListUser, schema2);
        userDataFrame.createOrReplaceTempView("user_info");
        // todo 使用方法
        Dataset<Row> resultsUser = sparkSession.sql("SELECT * FROM user_info limit 10");
        resultsUser.show();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("testDate2").config("spark.some.config.option","some value").master("local").getOrCreate();
        mock(spark);
    }



}
