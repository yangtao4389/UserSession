package cn.angetech.mockData;

import cn.angetech.util.DateUtils;
import cn.angetech.util.StringUtils;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

// 参考http://spark.apache.org/docs/latest/sql-getting-started.html#programmatically-specifying-the-schema
// 参考代码：JavaSparkSQLExample
public class MockData {
    public static void mock() {
        System.out.println("mock data");
        SparkSession spark = SparkSession.builder().appName("java spark sql basic example").config("spark.some.config.option","some value").master("local").getOrCreate();
        List<Row> rows = new ArrayList<Row>();
        String date = DateUtils.getTodayDate();  // 2019-12-12
        String[] actions = new String[]{"search", "click", "order", "pay"};
        String[] searchKeywords = new String[] {"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        Random random = new Random();

        for(int i=0;i<100;i++){
            long userid = random.nextInt(100);

            for(int j = 0;j<10;j++){
                String sessionid = UUID.randomUUID().toString().replace("-","");

                String baseActionTime = date + " " + StringUtils.fulfuill(String.valueOf(random.nextInt(23)));

                for(int k=0;k<random.nextInt(100);k++){
                    long pageid = random.nextInt(10);
                    String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))+ ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickCategoryId = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    if("search".equals(action)){
                        searchKeyword = searchKeywords[random.nextInt(10)];
                    }else if("click".equals(action)){
                        clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                    } else if("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }

                    Row row = RowFactory.create(date, userid, sessionid,
                            pageid, action, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds);
                    rows.add(row);
                }
            }
        }
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
        System.out.println(rows);
        // Apply the schema to the RDD
        Dataset<Row> userVisitDataFrame = spark.createDataFrame(rows, schema);

        // Creates a temporary view using the DataFrame
        userVisitDataFrame.createOrReplaceTempView("user_visit_action");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT * FROM user_visit_action");
        System.out.println(results);
        results.show();

        /*------------------------------------------------------------*/

        rows.clear();
        String[] sexes = new String[]{"male", "female"};
        for(int i = 0; i < 100; i ++) {
            long userid = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userid, username, name, age,
                    professional, city, sex);
            rows.add(row);
        }
        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)));

        Dataset<Row> userDataFrame = spark.createDataFrame(rows, schema2);
        userDataFrame.createOrReplaceTempView("user_info");

    }

    public static void main(String[] args) {
        mock();
    }

}
