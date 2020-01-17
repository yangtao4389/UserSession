package cn.angetech.session;

import cn.angetech.constant.Constants;
import cn.angetech.dao.factory.DaoFactory;
import cn.angetech.domain.SessionAggrStat;
import cn.angetech.mockData.MockData2;
import cn.angetech.util.DateUtils;
import cn.angetech.util.NumberUtils;
import cn.angetech.util.ParamUtils;
import cn.angetech.util.StringUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class UserVisitAnalyze3 {
    public static void main(String[] args) throws Exception{
        // todo 1.创建全局sparksession与javasparkcontext
        SparkSession ss = SparkSession.builder().appName("UserVisitAnalyze2").config("spark.some.config.option","some value").master("local").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());  // todo https://stackoverflow.com/questions/42582951/get-javasparkcontext-from-a-sparksession 如何创建javasprakcontext

        //todo 2.生成模拟数据
        MockData2.mock(ss);

        //todo 3.获取指定时间范围内的模拟数据  sessionRangeDateJavaRDD
        Long taskId = 1L;
        String taskParsm = "{\"startDate\":[\"2020-01-07\"],\"endDate\":[\"2020-01-29\"],\"startAge\":[20],\"endAge\":[30]}";
        JSONObject jsonObject = JSONObject.parseObject(taskParsm);
        JavaRDD<Row> sessionRangeDateJavaRDD = getActionRDD(ss,jsonObject);

        //todo 4.通过sessionRangeDateJavaRDD  获取访问过的所有品类 <catagoryId,catagoryId>
        JavaPairRDD<Long,Long> allCategoryId = getAllCategoryId(sessionRangeDateJavaRDD);
        allCategoryId = allCategoryId.distinct(); //todo 这里需要去重，否则统计数据出现重复，并且数据不对。
        System.out.println("44444444444444444-111111111");
        List<Tuple2<Long,Long>> top3allCategoryId = allCategoryId.take(30);
        for(Tuple2<Long,Long> tuple2:top3allCategoryId){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }

        //todo 5.统计点击品类、下单品类、支付品类的次数
        JavaPairRDD<Long,Long> clickCategoryCountRDD = getClickCategoryCountRDD(sessionRangeDateJavaRDD);
        JavaPairRDD<Long,Long> orderCategoryCountRDD = getOrderCategoryCountRDD(sessionRangeDateJavaRDD);
        JavaPairRDD<Long,Long> payCategoryCountRDD = getPayCategoryCountRDD(sessionRangeDateJavaRDD);

        // join 各品类与它的点击、下单、支付次数 输出：<categoryId,categoryId=id|clickcount=1|ordercount=2|paycount=3>
        JavaPairRDD<Long,String> joinCategoryIdAndCountRDD = getjoinCategoryIdAndCount(allCategoryId,clickCategoryCountRDD,orderCategoryCountRDD,payCategoryCountRDD);
        System.out.println("55555555555555555555-111111111");
        List<Tuple2<Long,String>> top3joinCategoryIdAndCountRDD = joinCategoryIdAndCountRDD.take(30);
        for(Tuple2<Long,String> tuple2:top3joinCategoryIdAndCountRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //4------categoryId=4|clickCategoryIds=0|orderCategoryIds=0|payCategoryIds=18
        //1------categoryId=1|clickCategoryIds=5|orderCategoryIds=18|payCategoryIds=0
        //3------categoryId=3|clickCategoryIds=0|orderCategoryIds=18|payCategoryIds=18
        //5------categoryId=5|clickCategoryIds=0|orderCategoryIds=0|payCategoryIds=18
        //2------categoryId=2|clickCategoryIds=7|orderCategoryIds=18|payCategoryIds=0

        //todo 6.对结果进行排序处理 排序逻辑 点击>下单>支付  找最热的，而不是付款最多的。
        JavaPairRDD<CategorySortKey,String> sortKeyStringJavaPairRDD = sortCount(joinCategoryIdAndCountRDD);
        System.out.println("666666666666666666-111111111");
        List<Tuple2<CategorySortKey,String>> top3sortKeyStringJavaPairRDD= sortKeyStringJavaPairRDD.take(30);
        for(Tuple2<CategorySortKey,String> tuple2:top3sortKeyStringJavaPairRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //cn.angetech.session.CategorySortKey@2487e20------categoryId=2|clickCategoryIds=7|orderCategoryIds=18|payCategoryIds=0
        //cn.angetech.session.CategorySortKey@3c9f4376------categoryId=1|clickCategoryIds=5|orderCategoryIds=18|payCategoryIds=0
        //cn.angetech.session.CategorySortKey@308a9264------categoryId=3|clickCategoryIds=0|orderCategoryIds=18|payCategoryIds=18
        //cn.angetech.session.CategorySortKey@7da77305------categoryId=4|clickCategoryIds=0|orderCategoryIds=0|payCategoryIds=18
        //cn.angetech.session.CategorySortKey@3cdfbbef------categoryId=5|clickCategoryIds=0|orderCategoryIds=0|payCategoryIds=18

        //todo 7.存入数据库，省略。




    }
    private static JavaRDD<Row>  getActionRDD(SparkSession ss,JSONObject jsonObject){
        String startDate = ParamUtils.getParam(jsonObject, Constants.PARAM_STARTDATE);
        String endDate = ParamUtils.getParam(jsonObject, Constants.PARAM_ENDTDATE);
        Dataset<Row> dataset = ss.sql("SELECT * FROM user_visit_action where where date >= '"+startDate+"' and date <= '"+endDate+"'");
        dataset.show();
        return dataset.javaRDD();
    }

    private static JavaPairRDD<Long,Long> getAllCategoryId(JavaRDD<Row> javaRDD){
        JavaPairRDD<Long,Long> categoryRDD = javaRDD.flatMapToPair(new PairFlatMapFunction<Row, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Row row) throws Exception {

                List<Tuple2<Long,Long>> tuple2List = new ArrayList<>();
                // 获取点击品类
                if(!row.isNullAt(7)){
                    tuple2List.add(new Tuple2<>(row.getLong(7),row.getLong(7)));
                }
                // 下单品类
                if(!row.isNullAt(9)){
                    String orderCategoryIds = row.getString(9);
                    String[] orderCategoryIdsList = orderCategoryIds.split(",");
                    for(String categoryId:orderCategoryIdsList){
                        tuple2List.add(new Tuple2<>(Long.valueOf(categoryId),Long.valueOf(categoryId)));
                    }
                }
                // 支付
                if(!row.isNullAt(11)){
                    String orderCategoryIds = row.getString(11);
                    String[] orderCategoryIdsList = orderCategoryIds.split(",");
                    for(String categoryId:orderCategoryIdsList){
                        tuple2List.add(new Tuple2<>(Long.valueOf(categoryId),Long.valueOf(categoryId)));
                    }
                }

                return tuple2List.iterator();
            }
        });
        return categoryRDD;
    }

    private static JavaPairRDD<Long,Long> getClickCategoryCountRDD(JavaRDD<Row> javaRDD){
        JavaPairRDD<Long,Long> categoryCountRDD =javaRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if(!row.isNullAt(7)){
                    return true;
                }else {
                    return false;
                }
            }
        }).mapToPair(new PairFunction<Row, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Row row) throws Exception {
                Long categoryId = row.getLong(7);
                return new Tuple2<>(categoryId,1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        });
        return categoryCountRDD;
    }

    private static JavaPairRDD<Long,Long> getOrderCategoryCountRDD(JavaRDD<Row> javaRDD){
        JavaPairRDD<Long,Long> categoryCountRDD =javaRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if(!row.isNullAt(9)){
                    return true;
                }else {
                    return false;
                }
            }
        }).flatMapToPair(new PairFlatMapFunction<Row, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Row row) throws Exception {
                String categoryIds = row.getString(9);
                List<Tuple2<Long,Long>> list = new ArrayList<>();
                String[] categoryIdsList = categoryIds.split(",");
                for(String categoryId:categoryIdsList){
                    list.add(new Tuple2<>(Long.valueOf(categoryId),1L));
                }
                return list.iterator();
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        });
        return categoryCountRDD;
    }

    private static JavaPairRDD<Long,Long> getPayCategoryCountRDD(JavaRDD<Row> javaRDD){
        JavaPairRDD<Long,Long> categoryCountRDD =javaRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if(!row.isNullAt(11)){
                    return true;
                }else {
                    return false;
                }
            }
        }).flatMapToPair(new PairFlatMapFunction<Row, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Row row) throws Exception {
                String categoryIds = row.getString(11);
                List<Tuple2<Long,Long>> list = new ArrayList<>();
                String[] categoryIdsList = categoryIds.split(",");
                for(String categoryId:categoryIdsList){
                    list.add(new Tuple2<>(Long.valueOf(categoryId),1L));
                }
                return list.iterator();
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        });
        return categoryCountRDD;
    }


    private static JavaPairRDD<Long,String> getjoinCategoryIdAndCount(JavaPairRDD<Long,Long> allCategoryId,JavaPairRDD<Long,Long> clickCategoryCountRDD,JavaPairRDD<Long,Long> orderCategoryCountRDD,JavaPairRDD<Long,Long> payCategoryCountRDD){
        JavaPairRDD<Long,String> joinClickRDD = allCategoryId.leftOuterJoin(clickCategoryCountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> longTuple2Tuple2) throws Exception {
                Long categoryId = longTuple2Tuple2._1;
                Long count = longTuple2Tuple2._2._2.isPresent()?longTuple2Tuple2._2._2.get():0L;
                String value = Constants.FIELD_CATEGORY_ID+"="+categoryId
                        +"|" + Constants.FIELD_CLICK_CATEGORYIDS+"="+String.valueOf(count);
                return new Tuple2<>(categoryId,value);
            }
        });
        JavaPairRDD<Long,String> joinOrderRDD = joinClickRDD.leftOuterJoin(orderCategoryCountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> longTuple2Tuple2) throws Exception {
                Long categoryId = longTuple2Tuple2._1;
                String value = longTuple2Tuple2._2._1;
                Long count = longTuple2Tuple2._2._2.isPresent()?longTuple2Tuple2._2._2.get():0L;
                value = value
                        +"|" + Constants.FIELD_ORDER_CATEGORYIDS+"="+String.valueOf(count);
                return new Tuple2<>(categoryId,value);
            }
        });
        JavaPairRDD<Long,String> joinPayRDD = joinOrderRDD.leftOuterJoin(payCategoryCountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> longTuple2Tuple2) throws Exception {
                Long categoryId = longTuple2Tuple2._1;
                String value = longTuple2Tuple2._2._1;
                Long count = longTuple2Tuple2._2._2.isPresent()?longTuple2Tuple2._2._2.get():0L;
                value = value
                        +"|" + Constants.FIELD_PAY_CATEGORYIDS+"="+String.valueOf(count);
                return new Tuple2<>(categoryId,value);
            }
        });

        return joinPayRDD;

    }


    private static JavaPairRDD<CategorySortKey,String> sortCount(JavaPairRDD<Long,String> javaPairRDD){
        // 首先映射成<sortKey，countInfo>
        // todo sortKey 为自定义的排序类：CategorySortKey
        JavaPairRDD<CategorySortKey,String> mapToCategorySortKey = javaPairRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> longStringTuple2) throws Exception {
                String countinfo = longStringTuple2._2;
                long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countinfo,"\\|",Constants.FIELD_CLICK_CATEGORYIDS));
                long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countinfo,"\\|",Constants.FIELD_ORDER_CATEGORYIDS));
                long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countinfo,"\\|",Constants.FIELD_PAY_CATEGORYIDS));

                CategorySortKey categorySortKey = new CategorySortKey();
                categorySortKey.set(clickCount,orderCount,payCount);
                return new Tuple2<>(categorySortKey,countinfo);

            }
        });
        JavaPairRDD<CategorySortKey,String> sortKeyStringJavaPairRDD = mapToCategorySortKey.sortByKey(false);
        return sortKeyStringJavaPairRDD;
    }


}
