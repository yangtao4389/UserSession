package cn.angetech.session;

import cn.angetech.conf.ConfigurationManager;
import cn.angetech.constant.Constants;
import cn.angetech.dao.TaskDao;
import cn.angetech.dao.factory.DaoFactory;
import cn.angetech.domain.*;
import cn.angetech.mockData.MockData;
import cn.angetech.util.DateUtils;
import cn.angetech.util.ParamUtils;
import cn.angetech.util.StringUtils;
import cn.angetech.util.ValidUtils;
import com.alibaba.fastjson.JSONObject;
import com.sun.jersey.core.util.StringIgnoreCaseKeyComparator;
import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.WindowFunctionType;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.util.AccumulatorV2;
import org.codehaus.janino.Java;
import org.stringtemplate.v4.ST;
import scala.Tuple2;
import org.apache.spark.api.java.Optional;
import java.util.*;
import java.util.function.LongFunction;

public class UserVisitAnalyze {
    public static void main(String[] args) {
        // todo 在2.0高版本的spark中，提供了SprakSession来整合所有的sqlcontext，hivecontext。全局使用sparksession
        SparkSession ss = SparkSession.builder().appName("java spark sql basic example").config("spark.some.config.option","some value").master("local").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());  // todo https://stackoverflow.com/questions/42582951/get-javasparkcontext-from-a-sparksession 如何创建javasprakcontext
//        SparkConf sparkConf = new SparkConf().setAppName(Constants.APP_NAME_SESSION).setMaster("local[3]");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        SparkSession sparkSession = SparkSession.builder.config(sparkConf);
//        SQLContext sqlContext = getSQLContext(javaSparkContext.sc());

        // 生成模拟数据
         MockData.mock(ss);

        //获取请求的taskid, 从数据库中查询到请求的参数
//        TaskDao dao = DaoFactory.getTaskDao();
        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        if(taskId == null){
            taskId =2L;
        }

//        // 从数据库中查询出相应的task
//        Task task = dao.findTaskById(taskId);
        // {"startDate":["2020-01-07"],"endDate":["2020-01-29"],"startAge":[20],"endAge":[30]}
//        System.out.println("task.getTaskParam:"+task.getTaskParam());
        String taskParsm = "{\"startDate\":[\"2020-01-07\"],\"endDate\":[\"2020-01-29\"],\"startAge\":[20],\"endAge\":[30]}";
        JSONObject jsonObject = JSONObject.parseObject(taskParsm);
        System.out.println("jsonObject"+jsonObject);
        // 开始写聚合
        // 获取指定范围内的session,从模拟数据中获取
        JavaRDD<Row> sessionRangeDate = getActionRDD(ss,jsonObject);
        JavaPairRDD<String,Row>  sessionInfoPairRDD = sessionRangeDate.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String,Row>(row.getString(2),row);
            }
        });
        // 查看3条得到的数据：
        List<Tuple2<String,Row>> top3SessionInfoPairRDD = sessionInfoPairRDD.take(3);
        System.out.println("1111111111111111111111111111111");
        for(Tuple2<String,Row> tuple2:top3SessionInfoPairRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //  date|user_id|          session_id|page_id|action|        action_time|search_keyword|click_category_id|click_product_id|order_category_ids|order_product_ids|pay_category_ids|pay_product_ids|
        // f7b3b624ca624dd4ba82586bb6236d56------[2020-01-10,81,f7b3b624ca624dd4ba82586bb6236d56,7,search,2020-01-10 20:35:28,蛋糕,null,null,null,null,null,null]
        // 重复用到的RDD 进行持久化
        sessionInfoPairRDD.persist(StorageLevel.DISK_ONLY());
        System.out.println("sessionInfoPairRDD done");

        // 按照session进行聚合
        JavaPairRDD<String,Iterable<Row>> sessionActionGrouped = sessionInfoPairRDD.groupByKey();

        // 查看
        List<Tuple2<String,Iterable<Row>>> top3sessionActionGrouped = sessionActionGrouped.take(1);
        System.out.println("2222222222222222222222222222222222222");
        for(Tuple2<String,Iterable<Row>> tuple2:top3sessionActionGrouped){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //158a178063b2414ea5fd4c6ff4df06a8------[[2020-01-10,43,158a178063b2414ea5fd4c6ff4df06a8,2,search,2020-01-10 01:14:17,蛋糕,null,null,null,null,null,null], [2020-01-10,43,158a178063b2414ea5fd4c6ff4df06a8,0,order,2020-01-10 01:08:53,null,null,null,75,0,null,null],,,]

        JavaPairRDD<Long,String> sessionPartInfo = sessionActionGrouped.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String sessionId = stringIterableTuple2._1;
                Iterable<Row> rows = stringIterableTuple2._2;

                StringBuffer searchKeyWords = new StringBuffer();
                StringBuffer clickCategoryIds = new StringBuffer();
                Long userId = null;
                Date startTime = null;
                Date endTime = null;

                int stepLength = 0;
                for(Row row:rows){
                    if(userId == null && !row.isNullAt(1)){
                        userId = row.getLong(1);
                    }

                    // searchKeyword
                    if(!row.isNullAt(6)){
                        String searchKeyword = row.getString(6);
                        if(!searchKeyWords.toString().contains(searchKeyword)){
                            searchKeyWords.append(searchKeyword+",");
                        }
                    }
                    if(!row.isNullAt(7)){
                        Long clickCategoryId = row.getLong(7);
                        if(!clickCategoryIds.toString().contains(String.valueOf(clickCategoryId))){
                            clickCategoryIds.append(String.valueOf(clickCategoryId)+",");
                        }
                    }

                    // 计算session开始时间和结束时间
                    Date actionTime = DateUtils.parseTime(row.getString(5));
                    if(startTime==null)
                        startTime=actionTime;
                    if(endTime==null)
                        endTime=actionTime;
                    if(actionTime.before(startTime))
                    {
                        startTime=actionTime;
                    }
                    if(actionTime.after(endTime))
                    {
                        endTime=actionTime;
                    }
                    stepLength ++;


                }

                Long visitLength=(endTime.getTime()-startTime.getTime())/1000;
                String searchKeywordsInfo=StringUtils.trimComma(searchKeyWords.toString());
                String clickCategoryIdsInfo=StringUtils.trimComma(clickCategoryIds.toString());
                String info = Constants.FIELD_SESSIONID+"="+sessionId+"|"
                        +Constants.FIELD_SERACH_KEYWORDS+"="+searchKeywordsInfo+"|"
                        +Constants.FIELD_CLICK_CATEGORYIDS+"="+clickCategoryIdsInfo+"|"
                        +Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"
                        +Constants.FIELD_STEP_LENGTH+"="+stepLength+"|"
                        +Constants.FIELD_START_TIME+"="+DateUtils.formatTime(startTime);
                return new Tuple2<Long,String>(userId,info);
            }
        });

        // 查看
        List<Tuple2<Long,String>> top3sessionPartInfo = sessionPartInfo.take(10);
        System.out.println("333333333333333333333333333333333");
        for(Tuple2<Long,String> tuple2:top3sessionPartInfo){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        // userid -------sessionId=      | ***
        //63------sessionId=50305d04d7934922a9dca54878d28a32|searchKeywords=太古商场,蛋糕,国贸大厦,重庆小面,日本料理|clickCategoryIds=71,79,38,12|visitLength=3318|stepLength=18|startTime=2020-01-10 01:00:28

        // 获取用户数据
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = ss.sql(sql).javaRDD();
        JavaPairRDD<Long,Row> userInfoPairRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long,Row>(row.getLong(0),row);
            }
        });
        // 查看
        List<Tuple2<Long,Row>> top3userInfoPairRDD = userInfoPairRDD.take(3);
        System.out.println("4444444444444444444444");
        for(Tuple2<Long,Row> tuple2:top3userInfoPairRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        // 0------[0,user0,name0,0,professional8,city75,male]

        //todo join userInfoPairRDD 与 sessionPartInfo
        JavaPairRDD<Long,Tuple2<String,Row>> userInfoJoinSessionPartInfoJavaPairRDD = sessionPartInfo.join(userInfoPairRDD);
        JavaPairRDD<String,String> userInfoJoinSessionPartInfoJavaPairRDD2 = userInfoJoinSessionPartInfoJavaPairRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> longTuple2Tuple2) throws Exception {
                String sessionPartInfo = longTuple2Tuple2._2._1;
                Row userInfo = longTuple2Tuple2._2._2;
                int age = userInfo.getInt(3);
                String professional = userInfo.getString(4);
                String city = userInfo.getString(5);
                String sex = userInfo.getString(6);

                // 拼接字符串
                String fullInfo = sessionPartInfo + "|" + Constants.FIELD_AGE+"="+age+"|"
                        +Constants.FIELD_PROFESSIONAL+"="+professional+"|"
                        +Constants.FIELD_CITY+"="+city+"|"
                        +Constants.FIELD_SEX+"="+sex;
                String session = StringUtils.getFieldFromConcatString(sessionPartInfo,"\\|",Constants.FIELD_SESSIONID);
                return new Tuple2<String,String>(session,fullInfo);

            }
        });
        // 查看
        List<Tuple2<String,String>> top3userInfoJoinSessionPartInfoJavaPairRDD2 = userInfoJoinSessionPartInfoJavaPairRDD2.take(3);
        System.out.println("555555555555555555555555");
        for(Tuple2<String,String> tuple2:top3userInfoJoinSessionPartInfoJavaPairRDD2){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //4cf838b731f94dbb90d5a3324d8a30db------sessionId=4cf838b731f94dbb90d5a3324d8a30db|searchKeywords=新辣道鱼火锅,呷哺呷哺,蛋糕|clickCategoryIds=95,41,16|visitLength=2914|stepLength=15|startTime=2020-01-10 19:02:21|age=46|professional=professional20|city=city75|sex=male



        //todo 重构+统计
        SessionAggrStatAccumulator sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
        ss.sparkContext().register(sessionAggrStatAccumulator,"AccumulatorV2:");
        System.out.println("sessionAggrStatAccumulator done");

        // 筛选符合条件的RDD
        JavaPairRDD<String,String> filteredSessionRDD = filterSessionAndAggrStat(userInfoJoinSessionPartInfoJavaPairRDD2,jsonObject,sessionAggrStatAccumulator);
        // 查看
        List<Tuple2<String,String>> top3filteredSessionRDD = filteredSessionRDD.take(3);
        System.out.println("6666666666666666666666666");
        for(Tuple2<String,String> tuple2:top3filteredSessionRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        // 22d3596214e341329dcc505318053d3a------sessionId=22d3596214e341329dcc505318053d3a|searchKeywords=火锅,呷哺呷哺,日本料理|clickCategoryIds=17,76,57,63|visitLength=3290|stepLength=18|startTime=2020-01-10 15:03:17|age=23|professional=professional26|city=city84|sex=female
        // 查看sessionAggrStatAccumulator,但好像查询的时候并没有处理完
        System.out.println("sessionAggrStatAccumulator.value():"+sessionAggrStatAccumulator.value());
        // session_count=3|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=3|1_3=0|4_6=0|7_9=0|10_30=3|30_60=0|60=0


        //获取符合过滤条件的全信息公共RDD
        // filteredSessionRDD:  22d3596214e341329dcc505318053d3a------sessionId=22d3596214e341329dcc505318053d3a|searchKeywords=火锅,呷哺呷哺,日本料理|clickCategoryIds=17,76,57,63|visitLength=3290|stepLength=18|startTime=2020-01-10 15:03:17|age=23|professional=professional26|city=city84|sex=female
        // sessionInfoPairRDD:  f7b3b624ca624dd4ba82586bb6236d56------[2020-01-10,81,f7b3b624ca624dd4ba82586bb6236d56,7,search,2020-01-10 20:35:28,蛋糕,null,null,null,null,null,null]
        JavaPairRDD<String, Row> commonFullClickInfoRDD=getFilterFullInfoRDD(filteredSessionRDD,sessionInfoPairRDD);
        //重复用到的RDD进行持久化
        commonFullClickInfoRDD.persist(StorageLevel.DISK_ONLY());
        // 查看
        List<Tuple2<String,Row>> top3commonFullClickInfoRDD = commonFullClickInfoRDD.take(3);
        System.out.println("777777777777777777777777");
        for(Tuple2<String,Row> tuple2:top3commonFullClickInfoRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        // 8487de01283d4492b7b56317efb06761------[2020-01-13,82,8487de01283d4492b7b56317efb06761,6,pay,2020-01-13 22:07:20,null,null,null,null,null,35,68]

        randomExtractSession(taskId,filteredSessionRDD,sessionInfoPairRDD);


        //获取热门品类数据Top10
//        getTop10Category(taskId,commonFullClickInfoRDD);
        List<Tuple2<CategorySortKey,String>> top10CategoryIds=getTop10Category(taskId,commonFullClickInfoRDD);
//        //获取热门每一个品类点击Top10session
        getTop10Session(jsc,taskId,sessionInfoPairRDD,top10CategoryIds);


    }

    /*
    * 通过java spark sql 来获取数据
    * */
    private static JavaRDD<Row> getActionRDD(SparkSession sparkSession, JSONObject taskParam){
        String startTime = ParamUtils.getParam(taskParam, Constants.PARAM_STARTTIME);
        String endTime = ParamUtils.getParam(taskParam, Constants.PARAM_ENDTIME);
        String sql = "select * from user_visit_action where date >='"+startTime+"' and date<='"+endTime+"'";
        Dataset<Row> df = sparkSession.sql(sql);
        //df.show();
        return df.javaRDD();
    }

//    /**
//     * 将数据进行映射成为Pair，键为SessionId，Value为Row
//     * @param sessionRangeDate
//     * @return
//     */
//     private static JavaPairRDD<String,Row> getSessionInfoPairRDD(JavaRDD<Row> sessionRangeDate){
//         sessionRangeDate.mapToPair(new PairFunction<Row, Object, Object>() {
//         })
//     }
//
//
//    private static JavaPairRDD<String,Row> getSessionInfoPairRDD(JavaRDD<Row> sessionRangeDate ){
//        return sessionRangeDate.mapToPair(new PairFunction<Row, String, Row>() {
//            @Override
//            public Tuple2<String, Row> call(Row row) throws Exception {
//               // System.out.println(row.getString(2)+row);
//                return new Tuple2<String, Row>(row.getString(2),row);
//            }
//        });
//    }


//    private static JavaPairRDD<String,String> aggregateBySessionId(SparkSession sc,JavaPairRDD<String,Row> sessionInfoPairRDD ){
//        JavaPairRDD<String,Iterable<Row>> sessionActionGrouped = sessionInfoPairRDD.groupByKey(); // key进行集合，value聚合成Iterable
//
//        JavaPairRDD<Long,String> sessionPartInfo = sessionActionGrouped.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
//            @Override
//            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
//                String sessionId = stringIterableTuple2._1;
//                Iterable<Row> rows = stringIterableTuple2._2;
//                System.out.println(sessionId);
//                System.out.println(rows.toString());  // [2020-01-08,33,ca3c94bfe4754580be0bf6468a9759a0,1,click,2020-01-08 19:16:44,null,9,28,null,null,null,null]
//                StringBuffer searchKeyWords = new StringBuffer();
//                StringBuffer clickCategoryIds = new StringBuffer();
//                Long userId = null;
//                Date startTime = null;
//                Date endTime = null;
//                int stepLength = 0;
//                for(Row row:rows){
//                    if(userId == null){
//                        userId = row.getLong(1);
//                    }
//                    String searchKeyword = row.getString(5);
//                    Long clickCategoryId = row.getLong(6);
//
//                    // 判断是否需要拼接
//                    if(StringUtils.isNotEmpty(searchKeyword)){
//                        if(!searchKeyWords.toString().contains(searchKeyword)){
//                            searchKeyWords.append(searchKeyword+",");
//                        }
//                    }
//                    if(clickCategoryId != null){
//                        if(!clickCategoryId.toString().contains(String.valueOf(clickCategoryId))){
//                            clickCategoryIds.append(String.valueOf(clickCategoryId)+",");
//                        }
//                    }
//
//                    // 计算session开始于结束时间
//                    Date actionTime = DateUtils.parseTime(row.getString(4));
//                    if(startTime == null){
//                        startTime = actionTime;
//                    }
//                    if(endTime == null){
//                        endTime = actionTime;
//                    }
//                    if(actionTime.before(startTime)){
//                        startTime = actionTime;
//                    }
//                    if(actionTime.after(endTime)){
//                        endTime = actionTime;
//                    }
//                    stepLength++;
//                }
//                // 访问时长
//                Long visitLength = (endTime.getTime() - startTime.getTime())/1000;
//
//                String searchKeywordsInfo = StringUtils.trimComma(searchKeyWords.toString());
//                String clickCategoryIdsInfo = StringUtils.trimComma(clickCategoryIds.toString());
//                String info = Constants.FIELD_SESSIONID+"="+sessionId+"|"
//                        +Constants.FIELD_SERACH_KEYWORDS+"="+searchKeywordsInfo+"|"
//                        +Constants.FIELD_CLICK_CATEGORYIDS+"="+clickCategoryIdsInfo+"|"
//                        +Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"
//                        +Constants.FIELD_STEP_LENGTH+"="+stepLength+"|"
//                        +Constants.FIELD_START_TIME+"="+DateUtils.formatTime(startTime);
//
//                return new Tuple2<Long,String>(userId,info);
//            }
//        });
//        // 查询所有的用户数据
//        String sql = "select * from user_info";
//        JavaRDD<Row> userInfoRDD = sc.sql(sql).javaRDD();
//        // 将用户信息隐射成map
//        JavaPairRDD<Long, Row> userInfoPariRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
//            @Override
//            public Tuple2<Long, Row> call(Row row) throws Exception {
//                return new Tuple2<Long,Row>(row.getLong(0),row );
//            }
//        });
//        // 将两个信息join
//        JavaPairRDD<Long, Tuple2<String,Row>> tuple2JavaPairRDD = sessionPartInfo.join(userInfoPariRDD);
//
//        // 拿到所需的session
//        JavaPairRDD<String,String> sessionInfo = tuple2JavaPairRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
//            @Override
//            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> longTuple2Tuple2) throws Exception {
//                String sessionPartInfo = longTuple2Tuple2._2._1;
//                Row userInfo = longTuple2Tuple2._2._2;
//                // 拿到需要的用户信息
//                int age = userInfo.getInt(3);
//                String professional = userInfo.getString(4);
//                String city = userInfo.getString(5);
//                String sex = userInfo.getString(6);
//
//                // 拼接字符串
//                String fullInfo = sessionPartInfo + "|" + Constants.FIELD_AGE+"="+age+"|"
//                        +Constants.FIELD_PROFESSIONAL+"="+professional+"|"
//                        +Constants.FIELD_CITY+"="+city+"|"
//                        +Constants.FIELD_SEX+"="+sex;
//                String session = StringUtils.getFieldFromConcatString(sessionPartInfo,"|",Constants.FIELD_SESSIONID);
//                return new Tuple2<String,String>(session,fullInfo);
//            }
//
//        });
//        return sessionInfo;
//    }


    private static JavaPairRDD<String,String> filterSessionAndAggrStat(JavaPairRDD<String,String> sessionInfoRDD, final JSONObject taskParam, final AccumulatorV2 sessionAggrStatAccumulator){
        String startAge = ParamUtils.getParam(taskParam,Constants.PARAM_STARTAGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_ENDAGE);
        String professionals = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSONALS);
        String cities = ParamUtils.getParam(taskParam,Constants.PARAM_CIYTIES);
        String sex = ParamUtils.getParam(taskParam,Constants.PARAM_SEX);
        String keyWords = ParamUtils.getParam(taskParam,Constants.PARAM_SERACH_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam,Constants.PARAM_CLICK_CATEGORYIDS);

        // 拼接参数
        String _paramter = (startAge!=null?Constants.PARAM_STARTAGE+"="+startAge+"|":"")+
                (endAge!=null?Constants.PARAM_ENDAGE+"="+endAge+"|":"")+
                (professionals!=null?Constants.PARAM_PROFESSONALS+"="+professionals+"|":"")+
                (cities!=null?Constants.PARAM_CIYTIES+"="+cities+"|":"")+
                (sex!=null?Constants.PARAM_SEX+"="+sex+"|":"")+
                (keyWords!=null?Constants.PARAM_SERACH_KEYWORDS+"="+keyWords+"|":"")+
                (categoryIds!=null?Constants.PARAM_CLICK_CATEGORYIDS+"="+categoryIds+"|":"");
        if(_paramter.endsWith("\\|")){
            _paramter = _paramter.substring(0,_paramter.length()-1);
        }
        final String paramter = _paramter;

        JavaPairRDD<String,String> filteredSessionRDD = sessionInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String sessionInfo = stringStringTuple2._2;
                // 按照条件进行过滤
                // 按照年龄进行过滤
                if(!ValidUtils.between(sessionInfo,Constants.FIELD_AGE,paramter,Constants.PARAM_STARTAGE,Constants.PARAM_ENDAGE)){
                    return false;
                }
                // 按照职业过滤
                if(!ValidUtils.in(sessionInfo,Constants.FIELD_PROFESSIONAL,paramter,Constants.PARAM_PROFESSONALS)){
                    return false;
                }
                // 按照城市进行过滤
                if(!ValidUtils.in(sessionInfo,Constants.FIELD_CITY,paramter,Constants.PARAM_CIYTIES)){
                    return false;
                }
                // 按照性别进行筛选
                if(!ValidUtils.equal(sessionInfo,Constants.FIELD_SEX,paramter,Constants.PARAM_SEX)){
                    return false;
                }

                // 按照搜索词进行过滤，只要有一个搜索词即可
                if(!ValidUtils.in(sessionInfo,Constants.FIELD_SERACH_KEYWORDS,paramter,Constants.PARAM_SERACH_KEYWORDS)){
                    return false;
                }
                if(!ValidUtils.in(sessionInfo,Constants.FIELD_CLICK_CATEGORYIDS,paramter,Constants.FIELD_CLICK_CATEGORYIDS)){
                    return false;
                }

                // 满足条件的
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                Long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.FIELD_VISIT_LENGTH));
                Long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.FIELD_STEP_LENGTH));

                // 使用函数进行统计
                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);
                return true;

            }
            private void calculateVisitLength(Long visitLegth){
                if(visitLegth>=1&&visitLegth<=3)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                else if(visitLegth>=4&&visitLegth<=6)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                else if(visitLegth>=7&&visitLegth<=9)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                else if(visitLegth>=10&&visitLegth<=30)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                else if(visitLegth>30&&visitLegth<=60)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                else if(visitLegth>60&&visitLegth<=180)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                else if(visitLegth>180&&visitLegth<=600)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                else if(visitLegth>600&&visitLegth<=1800)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                else if(visitLegth>1800)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
            }

            private void calculateStepLength(Long stepLength){
                if(stepLength>=1&&stepLength<=3)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                else if(stepLength>=4&&stepLength<=6)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                else if(stepLength>=7&&stepLength<=9)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                else if(stepLength>=10&&stepLength<=30)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                else if(stepLength>30&&stepLength<=60)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                else if(stepLength>60)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
            }

        });
        return filteredSessionRDD;


    }


    private static JavaPairRDD<String,Row> getFilterFullInfoRDD(JavaPairRDD<String, String> filteredSessionRDD, JavaPairRDD<String, Row> sessionInfoPairRDD){
        return filteredSessionRDD.join(sessionInfoPairRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> stringTuple2Tuple2) throws Exception {
                return new Tuple2<String,Row>(stringTuple2Tuple2._1,stringTuple2Tuple2._2._2);
            }
        });
    }


    private static void randomExtractSession(Long taskId,JavaPairRDD<String, String> filteredSessionRDD, JavaPairRDD<String, Row> sessionInfoPairRDD){
        // filteredSessionRDD:  22d3596214e341329dcc505318053d3a------sessionId=22d3596214e341329dcc505318053d3a|searchKeywords=火锅,呷哺呷哺,日本料理|clickCategoryIds=17,76,57,63|visitLength=3290|stepLength=18|startTime=2020-01-10 15:03:17|age=23|professional=professional26|city=city84|sex=female
        // sessionInfoPairRDD:  f7b3b624ca624dd4ba82586bb6236d56------[2020-01-10,81,f7b3b624ca624dd4ba82586bb6236d56,7,search,2020-01-10 20:35:28,蛋糕,null,null,null,null,null,null]

        // 1. 先将过滤session进行映射，映射成Time，Info的数据格式
        //  (2020-01-10_15,info),(),()
        final JavaPairRDD<String,String> mapDataRDD = filteredSessionRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String info = stringStringTuple2._2;
                // 获取开始时间
                String startTime = StringUtils.getFieldFromConcatString(info,"\\|",Constants.FIELD_START_TIME);
                String formatStratTime = DateUtils.getDateHour(startTime);
                return new Tuple2<String,String>(formatStratTime,info);
            }
        });



        // 计算每一个小时的session数量
        // {"时间"：数量,  "2020-01-10_15":10,  }
        Map<String,Long> mapCount = mapDataRDD.countByKey();


        // 设计一个新的数据结构，日期作为key，时间和数量为map   Map<String, Map<String, Long>>
        Map<String,Map<String,Long>> dataHourCountMap = new HashMap<>();  // {"2020-01-20:{1:10,2:20,3:10....}}
        // 遍历mapcount
        for(Map.Entry<String,Long> entry:mapCount.entrySet()){
            System.out.println("mapCount:"+entry.getKey()+"----"+entry.getValue());  // 2020-01-13_09----10
            String date = entry.getKey().split("_")[0];
            String hour = entry.getKey().split("_")[1];

            Map<String,Long> hourCount = dataHourCountMap.get(date);
            if(hourCount == null){
                hourCount = new HashMap<String,Long>();
                dataHourCountMap.put(date,hourCount);
            }
            hourCount.put(hour,entry.getValue());
        }

        // 将数据按照天数平均
        int countPerday = 100/dataHourCountMap.size();
        Random random=new Random();
        // 设计一个新的数据结构，用于存储随机索引，key是每一天，map是小时和随机索引列表构成的。
        final Map<String,Map<String,List<Long>>> dataRandomExtractMap = new HashMap<String, Map<String,List<Long>>>();    // {"2020-01-20":{1:[1,3,4]}}
        for (Map.Entry<String,Map<String,Long>> dataHourCount: dataHourCountMap.entrySet()){
            System.out.println("dataHourCountMap:"+dataHourCount.getKey()+"----"+dataHourCount.getValue());  // 2020-01-13----{22=4, 11=8, 00=10, 12=5, 01=7, 02=11, 13=8, 03=4, 14=4, 15=7, 04=10, 16=8, 05=7, 17=7, 06=5, 07=9, 18=10, 19=5, 08=2, 09=10, 20=7, 10=9, 21=8}
            String date = dataHourCount.getKey();
            Long sessionDayCount = 0L; // 当天所有的session总和
            for(Map.Entry<String,Long> hourCountMap:dataHourCount.getValue().entrySet()){
                sessionDayCount += hourCountMap.getValue();
            }

            // 获取每一天随机存储的Map
            Map<String,List<Long>> dayExtractMap = dataRandomExtractMap.get(date);
            if(dayExtractMap == null){
                dayExtractMap = new HashMap<String,List<Long>>();
                dataRandomExtractMap.put(date,dayExtractMap);
            }
            // 遍历每一个小时，计算每一个小时的session占比和抽取的数量
            for(Map.Entry<String,Long> hourCountMap:dataHourCount.getValue().entrySet()){
                System.out.println("dataHourCount.getValue().entrySet():"+hourCountMap.getKey()+"----"+hourCountMap.getValue()); // :22----4
                int extractSize = (int)((double)hourCountMap.getValue()/sessionDayCount*countPerday) ;
                //如果抽离的长度大于被抽取数据的长度，那么抽取的长度就是被抽取长度
                extractSize = extractSize>hourCountMap.getValue()?hourCountMap.getValue().intValue():extractSize;
                System.out.println("extractSize:"+extractSize); // 4

                List<Long> indexList = dayExtractMap.get(hourCountMap.getKey());
                if(indexList == null){
                    indexList = new ArrayList<Long>();
                    dayExtractMap.put(hourCountMap.getKey(),indexList);
                }

                // 使用随机函数生成随机索引
                for(int i=0;i<extractSize;i++){
                    int index = random.nextInt(hourCountMap.getValue().intValue());
                    //如果包含，那么一直循环直到不包含为止
                    while (indexList.contains(Long.valueOf(index))){
                        index = random.nextInt(hourCountMap.getValue().intValue());
                    }
                    indexList.add(Long.valueOf(index));
                }

            }
        }

        // 查看dataRandomExtractMap
        for(Map.Entry<String,Map<String,List<Long>>> entry:dataRandomExtractMap.entrySet()){
            System.out.println("inner-000000000000000000000--dataRandomExtractMap:"+entry.getKey()+"----"+entry.getValue());
        }
        //dataRandomExtractMap:2020-01-13----{22=[4, 10, 11, 6, 0], 11=[2, 0, 8, 3], 00=[5, 2], 12=[4, 0, 2, 6, 1], 01=[8, 1, 4, 6, 7], 02=[0, 2, 3], 13=[7, 1, 3], 03=[11, 6, 7, 8, 14, 2], 14=[3, 1, 6], 15=[3, 0, 1], 04=[7, 1, 6, 4], 16=[0, 1, 7, 6], 05=[3, 0, 5, 9, 4], 17=[6, 3, 5], 06=[2, 5, 0], 07=[7, 0, 3, 6], 18=[8, 1, 4, 2], 19=[10, 5, 11, 12, 4], 08=[3, 1, 6, 9, 2], 09=[2, 4], 20=[1, 0, 4], 10=[2, 6, 1, 7], 21=[11, 9, 5, 1, 6]}


        //2.将上面计算的RDD进行分组，然后使用FlatMap进行压平，然后判断是否在索引中，如果在，那么将这个信息持久化
        JavaPairRDD<String,Iterable<String>> time2GroupRDD = mapDataRDD.groupByKey();

        System.out.println("inner--1-1-1-1-1-1-1-1-1--1-1-1-1-1--1");
        List<Tuple2<String,Iterable<String>>> top3time2GroupRDD = time2GroupRDD.take(3);
        for(Tuple2<String,Iterable<String>> tuple2:top3time2GroupRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        // 2020-01-13_22------[sessionId=07638aafc04f4846b2be8d1211eacf53|searchKeywords=太古商场,  sessionId=,,,,]

        // (2020-01-10_15,[info1,info2])
        //将抽取的信息持久化到数据库，并返回SessionIds对，然后和以前的信息Join
        JavaPairRDD<String,String> sessionIds = time2GroupRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                String dateStr = stringIterableTuple2._1;
                String date = dateStr.split("_")[0];
                String hour = dateStr.split("_")[1];
                // 使用一个list来存储sessionId
                List<Tuple2<String,String>> sessionIds = new ArrayList<>();
                List<Long> indexList = dataRandomExtractMap.get(date).get(hour);

                List<SessionRandomExtract> sessionRandomExtractList = new ArrayList<>();
                int index = 0;
                for(String infos:stringIterableTuple2._2){
                    if(indexList.contains(Long.valueOf(index))){
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        final String sessionId = StringUtils.getFieldFromConcatString(infos,"\\|",Constants.FIELD_SESSIONID);
                        String startTime=StringUtils.getFieldFromConcatString(infos,"\\|",Constants.FIELD_START_TIME);
                        String searchKeyWards=StringUtils.getFieldFromConcatString(infos,"\\|",Constants.FIELD_SERACH_KEYWORDS);
                        String clickCategoryIds=StringUtils.getFieldFromConcatString(infos,"\\|",Constants.FIELD_CLICK_CATEGORYIDS);
                        sessionRandomExtract.set(sessionId,startTime,searchKeyWards,clickCategoryIds);
                        sessionRandomExtractList.add(sessionRandomExtract);
                        sessionIds.add(new Tuple2<String,String>(sessionId,sessionId));
                    }
                    index ++;
                }
//                System.out.println("sessionRandomExtractList:"+sessionRandomExtractList);
//                System.out.println("sessionRandomExtractList:"+sessionRandomExtractList[0]);
                DaoFactory.getSessionRandomExtractDao().batchInsert(sessionRandomExtractList);
                return sessionIds.iterator();
            }
        });
        System.out.println("inner-11111111111111111111111");
        List<Tuple2<String,String>> top3sessionIds = sessionIds.take(1);
        for(Tuple2<String,String> tuple2:top3sessionIds){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        // ed1cdc9b34f04ed3b0e88cb0acb4bca0------ed1cdc9b34f04ed3b0e88cb0acb4bca0

        //3. 获取session的明细数据保存到数据库
        JavaPairRDD<String,Tuple2<String,Row>> sessionDetailRDD= sessionIds.join(sessionInfoPairRDD);

        System.out.println("inner-222222222222222222222");
        List<Tuple2<String,Tuple2<String,Row>>> top3sessionDetailRDD = sessionDetailRDD.take(3);
        for(Tuple2<String,Tuple2<String,Row>> tuple2:top3sessionDetailRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }

        sessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> tuple2Iterator) throws Exception {
                List<SessionDetail> sessionDetailList = new ArrayList<>();
                while (tuple2Iterator.hasNext()){
                    Tuple2<String,Tuple2<String,Row>> tuple2 = tuple2Iterator.next();
                    Row row = tuple2._2._2;
                    String sessionId = tuple2._1;
//                    System.out.println(row); //
                    //  0     1                 2      3       4             5            6              7                  8               9                  10                11               12
                    //  date|user_id|      session_id|page_id|action|        action_time|search_keyword|click_category_id|click_product_id|order_category_ids|order_product_ids|pay_category_ids|pay_product_ids|
                    // [2020-01-13,28,f59f79e06f1d45e186c89db4c5f615e1,1,order,2020-01-13 15:25:41,null,null,null,57,0,null,null]
                    Long userId=row.getLong(1);
                    Long pageId=row.getLong(3);
                    String actionTime=row.getString(5);
                    String searchKeyWard = null;
                    Long clickCategoryId= null;
                    Long clickProducetId= null;
                    String orderCategoryId = null;
                    String orderProducetId = null;
                    String payCategoryId = null;
                    String payProducetId = null;
                    if(!row.isNullAt(6)){
                         searchKeyWard=row.getString(6);
                    }
                    if(!row.isNullAt(7)){
                         clickCategoryId=row.getLong(7);
                    }
                    if(!row.isNullAt(8)){
                         clickProducetId=row.getLong(8);
                    }
                    if(!row.isNullAt(9)){
                         orderCategoryId=row.getString(9);
                    }
                    if(!row.isNullAt(10)){
                         orderProducetId=row.getString(10);
                    }
                    if(!row.isNullAt(11)){
                         payCategoryId=row.getString(11);
                    }
                    if(!row.isNullAt(12)){
                         payProducetId=row.getString(12);
                    }


                    SessionDetail sessionDetail=new SessionDetail();
                    sessionDetail.set(userId,sessionId,pageId,actionTime,searchKeyWard,clickCategoryId,clickProducetId,orderCategoryId,orderProducetId,payCategoryId,payProducetId);
                    sessionDetailList.add(sessionDetail);
                }
                DaoFactory.getSessionDetailDao().batchInsert(sessionDetailList);
            }
        });

    }

    /*
    sessionId2DetailRDD                      0               1           2           3     4               5             6           7                   8                9                   10             11               12
                                             date       |user_id|      session_id|page_id|action|        action_time|search_keyword|click_category_id|click_product_id|order_category_ids|order_product_ids|pay_category_ids|pay_product_ids|
    * 8487de01283d4492b7b56317efb06761------[2020-01-13,82,8487de01283d4492b7b56317efb06761,6,pay,2020-01-13 22:07:20,null,null,null,null,null,35,68]
    * */
//    private static List<Tuple2<CategorySortKey,String>> getTop10Category(Long taskId, JavaPairRDD<String,Row> sessionId2DetailRDD){
    private static  List<Tuple2<CategorySortKey,String>> getTop10Category(Long taskId, JavaPairRDD<String,Row> sessionId2DetailRDD){
        //1.第一步已抽离出方法getFilterFullInfoRDD
        //2。获取访问的品类id，访问过表示点击，下单，支付
        JavaPairRDD<Long,Long> categoryRDD = sessionId2DetailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Row row = stringRowTuple2._2;
                List<Tuple2<Long,Long>> visitCategoryList = new ArrayList<>();
                if(!row.isNullAt(7)){
                    Long clickCategoryId = row.getLong(7);
                    visitCategoryList.add(new Tuple2<Long,Long>(clickCategoryId,clickCategoryId));
                }
                if(!row.isNullAt(9)){
                    String[] orderCategoryIdsSplited = row.getString(9).split(",");
                    for(String orderCategoryId:orderCategoryIdsSplited){
                        visitCategoryList.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),Long.valueOf(orderCategoryId)));
                    }
                }
                if(!row.isNullAt(11)){
                    String[] payCategoryIdsSplited = row.getString(11).split(",");
                    for(String payCategoryId:payCategoryIdsSplited){
                        visitCategoryList.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),Long.valueOf(payCategoryId)));
                    }
                }
                return visitCategoryList.iterator();


            }
        });

        // 去重
        categoryRDD = categoryRDD.distinct();
        //3。计算各个品类的点击，下单和支付次数
        // 3.1 计算点击品类的数量
        JavaPairRDD<Long,Long> clickCategoryRDD = getLClickCategoryRDD(sessionId2DetailRDD);

        // 查看
        System.out.println("clickCategoryRDD-8888888888888888");
        List<Tuple2<Long,Long>> top3clickCategoryRDD = clickCategoryRDD.take(3);
        for(Tuple2<Long,Long> tuple2:top3clickCategoryRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }

        // 3.2 计算下单的品类的数量
        JavaPairRDD<Long,Long> orderCategoryRDD= getOrderCategoryRDD(sessionId2DetailRDD);
        // 查看
        System.out.println("orderCategoryRDD---999999999999999");
        List<Tuple2<Long,Long>> top3orderCategoryRDD = orderCategoryRDD.take(3);
        for(Tuple2<Long,Long> tuple2:top3orderCategoryRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }


//        // 3.3 计算支付的品类的数量
        JavaPairRDD<Long,Long> payCategoryRDD=getPayCategoryRDD(sessionId2DetailRDD);
        // 查看
        System.out.println("orderCategoryRDD---10000000000000000001010101");
        List<Tuple2<Long,Long>> top3payCategoryRDD = payCategoryRDD.take(3);
        for(Tuple2<Long,Long> tuple2:top3payCategoryRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //19------4
        //39------2
        //34------3

        //4.将上述计算的三个字段进行join，注意这里是LeftOuterJoin，因为有些品类只是点击了
        JavaPairRDD<Long,String> categoryCountRDD=joinCategoryAndData(categoryRDD,clickCategoryRDD,orderCategoryRDD,payCategoryRDD);
        // 查看
        System.out.println("orderCategoryRDD---11-11-11-11-11-114-11");
        List<Tuple2<Long,String>> top3categoryCountRDD = categoryCountRDD.take(3);
        for(Tuple2<Long,String> tuple2:top3categoryCountRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //39------categoryId=39|clickCategory=6|orderCategory=5|payCategory=3
        //19------categoryId=19|clickCategory=6|orderCategory=3|payCategory=5
        //34------categoryId=34|clickCategory=3|orderCategory=5|payCategory=4

        // 自定义二次排序的key
        JavaPairRDD<CategorySortKey,String>  sortKeyCountRDD = categoryCountRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> longStringTuple2) throws Exception {
                String countInfo = longStringTuple2._2;
                Long clickCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_CATEGORY));
                Long orderCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_CATEGORY));
                Long payCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_CATEGORY));
                CategorySortKey key=new CategorySortKey();
                key.set(clickCount,orderCount,payCount);
                return new Tuple2<CategorySortKey, String>(key,countInfo);
            }
        });
        // 查看
        System.out.println("sortKeyCountRDD---12-12-12-12-12-12-12");
        List<Tuple2<CategorySortKey,String>> top3sortKeyCountRDD = sortKeyCountRDD.take(3);
        for(Tuple2<CategorySortKey,String> tuple2:top3sortKeyCountRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //cn.angetech.session.CategorySortKey@aa8dce8------categoryId=39|clickCategory=6|orderCategory=5|payCategory=3
        //cn.angetech.session.CategorySortKey@6ad112de------categoryId=19|clickCategory=6|orderCategory=3|payCategory=5
        //cn.angetech.session.CategorySortKey@18a0721b------categoryId=34|clickCategory=3|orderCategory=5|payCategory=4

        // 排序
        JavaPairRDD<CategorySortKey,String> sortedCategoryRDD = sortKeyCountRDD.sortByKey(false);
        // 查看
        System.out.println("sortedCategoryRDD---131313113131131313131313131");
        List<Tuple2<CategorySortKey,String>> top3sortedCategoryRDD = sortedCategoryRDD.take(10);
        for(Tuple2<CategorySortKey,String> tuple2:top3sortedCategoryRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //cn.angetech.session.CategorySortKey@7037a680------categoryId=65|clickCategory=11|orderCategory=6|payCategory=3
        //cn.angetech.session.CategorySortKey@492c8137------categoryId=46|clickCategory=9|orderCategory=4|payCategory=4
        //cn.angetech.session.CategorySortKey@420b55ed------categoryId=87|clickCategory=9|orderCategory=1|payCategory=5
        //cn.angetech.session.CategorySortKey@f237ae7------categoryId=11|clickCategory=8|orderCategory=4|payCategory=4
        //cn.angetech.session.CategorySortKey@42edde25------categoryId=60|clickCategory=8|orderCategory=2|payCategory=3
        //cn.angetech.session.CategorySortKey@6fe5da76------categoryId=49|clickCategory=7|orderCategory=6|payCategory=4
        //cn.angetech.session.CategorySortKey@77d95e5a------categoryId=23|clickCategory=7|orderCategory=5|payCategory=4
        //cn.angetech.session.CategorySortKey@6339e604------categoryId=28|clickCategory=7|orderCategory=5|payCategory=5
        //cn.angetech.session.CategorySortKey@4f8900b0------categoryId=10|clickCategory=6|orderCategory=6|payCategory=2
        //cn.angetech.session.CategorySortKey@e7e455d------categoryId=39|clickCategory=6|orderCategory=5|payCategory=3

        // 取出前10 写入数据库
        List<Tuple2<CategorySortKey,String>> top10CategoryList=sortedCategoryRDD.take(1000);
        List<Top10Category> top10Categories = new ArrayList<>();
        for(Tuple2<CategorySortKey,String> tuple2:top10CategoryList){
            String countInfo = tuple2._2;
            Long categoryId=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID));
            Long clickCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_CATEGORY));
            Long orderCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_CATEGORY));
            Long payCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_CATEGORY));
            Top10Category top10Category=new Top10Category();
            top10Category.set(taskId,categoryId,clickCount,orderCount,payCount);
            top10Categories.add(top10Category);
        }
        DaoFactory.getTop10CategoryDao().batchInsert(top10Categories);
        return top10CategoryList;


    }

    private static JavaPairRDD<Long,Long>  getLClickCategoryRDD(JavaPairRDD<String,Row> sessionId2DetailRDD){
        // 筛选 click的
        JavaPairRDD<String,Row> clickActionRDD = sessionId2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Row row = stringRowTuple2._2;
                if(row.isNullAt(7)){
                    return false;
                }
                return true;
            }
        });
        // 映射成新的pair
        // {(30,1),(40,1)}
        JavaPairRDD<Long,Long> clickCategoryRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Long row = stringRowTuple2._2.getLong(7);
                return new Tuple2<Long,Long>(row,1L);
            }
        });

        // 计算次数
        JavaPairRDD<Long,Long> clickCategoryCountRDD = clickCategoryRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        });
        return clickCategoryCountRDD;

    }

    private static JavaPairRDD<Long,Long>  getOrderCategoryRDD(JavaPairRDD<String,Row> sessionId2DetailRDD){
        // 筛选 click的
        JavaPairRDD<String,Row> clickActionRDD = sessionId2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Row row = stringRowTuple2._2;
                if(row.isNullAt(9)){
                    return false;
                }
                return true;
            }
        });
        // 映射成新的pair
        // {(30,1),(40,1)}
        JavaPairRDD<Long,Long> clickCategoryRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Long row = Long.valueOf(stringRowTuple2._2.getString(9)) ;
                return new Tuple2<Long,Long>(row,1L);
            }
        });

        // 计算次数
        JavaPairRDD<Long,Long> clickCategoryCountRDD = clickCategoryRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        });
        return clickCategoryCountRDD;

    }

    private static JavaPairRDD<Long,Long>  getPayCategoryRDD(JavaPairRDD<String,Row> sessionId2DetailRDD){
        // 筛选 click的
        JavaPairRDD<String,Row> clickActionRDD = sessionId2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Row row = stringRowTuple2._2;
                if(row.isNullAt(11)){
                    return false;
                }
                return true;
            }
        });
        // 映射成新的pair
        // {(30,1),(40,1)}
        JavaPairRDD<Long,Long> clickCategoryRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Long row = Long.valueOf(stringRowTuple2._2.getString(11)) ;
                return new Tuple2<Long,Long>(row,1L);
            }
        });

        // 计算次数
        JavaPairRDD<Long,Long> clickCategoryCountRDD = clickCategoryRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        });
        return clickCategoryCountRDD;

    }

    private static  JavaPairRDD<Long,String>  joinCategoryAndData(JavaPairRDD<Long, Long> categoryRDD, JavaPairRDD<Long, Long> clickCategoryRDD, JavaPairRDD<Long, Long> orderCategoryRDD, JavaPairRDD<Long, Long> payCategoryRDD){
        JavaPairRDD<Long,Tuple2<Long,Optional<Long>>> tempJoinRDD = categoryRDD.leftOuterJoin(clickCategoryRDD);

        JavaPairRDD<Long,String> tmpRDD = tempJoinRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> longTuple2Tuple2) throws Exception {
                Long categoryId = longTuple2Tuple2._1;
                Optional<Long> clickOptional = longTuple2Tuple2._2._2;
                Long clickCount = 0L;
                if(clickOptional.isPresent()){
                    clickCount = clickOptional.get();
                }
                String value = Constants.FIELD_CATEGORY_ID+"="+categoryId+"|"+Constants.FIELD_CLICK_CATEGORY+"="+clickCount;
                return new Tuple2<Long, String>(categoryId,value);
            }
        });
        // join 下单的次数
        tmpRDD = tmpRDD.leftOuterJoin(orderCategoryRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> longTuple2Tuple2) throws Exception {
                Long categoryId = longTuple2Tuple2._1;
                Optional<Long> clickOptional = longTuple2Tuple2._2._2;
                Long clickCount = 0L;
                String value = longTuple2Tuple2._2._1;
                if(clickOptional.isPresent()){
                    clickCount = clickOptional.get();
                }
                value = value+"|"+Constants.FIELD_ORDER_CATEGORY+"="+clickCount;
                return  new Tuple2<Long, String>(categoryId,value);
            }
        });
        // join 支付的次数
        tmpRDD = tmpRDD.leftOuterJoin(payCategoryRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> longTuple2Tuple2) throws Exception {
                Long categoryId = longTuple2Tuple2._1;
                Optional<Long> clickOptional = longTuple2Tuple2._2._2;
                Long clickCount = 0L;
                String value = longTuple2Tuple2._2._1;
                if(clickOptional.isPresent()){
                    clickCount = clickOptional.get();
                }
                value=value+"|"+Constants.FIELD_PAY_CATEGORY+"="+clickCount;
                return new Tuple2<Long, String>(categoryId,value);
            }
        });
        return tmpRDD;

    }

    private static void getTop10Session(JavaSparkContext sc, Long taskId, JavaPairRDD<String, Row> sessionInfoPairRDD, List<Tuple2<CategorySortKey, String>> top10CategoryIds){
        List<Tuple2<Long,Long>> categoryIdList = new ArrayList<>();
        for(Tuple2<CategorySortKey, String>top10CategoryId:top10CategoryIds ){
            String countInfo=top10CategoryId._2;
            Long categoryId=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID));
            categoryIdList.add(new Tuple2<Long, Long>(categoryId,categoryId));
        }
        // 生成一份RDD
        JavaPairRDD<Long,Long> top10CategoryIdsRDD = sc.parallelizePairs(categoryIdList);
        // 按照sessionId进行分组
        JavaPairRDD<String,Iterable<Row>> groupBySessionIdRDD = sessionInfoPairRDD.groupByKey();

        // 计算每个session对品类的点击次数
        JavaPairRDD<Long,String> categorySessionCountRDD = groupBySessionIdRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String sessionId = stringIterableTuple2._1;
                // 保存每一个品类的单击次数
                Map<Long,Long> categoryIdCount = new HashMap<>();
                for(Row row:stringIterableTuple2._2){
                    if(!row.isNullAt(7)){
                        Long count = categoryIdCount.get(row.getLong(7));
                        if(count == null){
                            count = 0L;
                        }
                        count ++;
                        categoryIdCount.put(row.getLong(7),count);
                    }
                }

                // 数据拼接
                List<Tuple2<Long,String>> categoryIdCountList = new ArrayList<>();
                for(Map.Entry<Long,Long> entry:categoryIdCount.entrySet()){
                    String value = sessionId+","+entry.getValue();
                    categoryIdCountList.add(new Tuple2<>(entry.getKey(),value));
                }
                return categoryIdCountList.iterator();
            }
        });

        // 查看
        System.out.println("categorySessionCountRDD-1414141414141414141414");
        List<Tuple2<Long,String>> top3categorySessionCountRDD = categorySessionCountRDD.take(3);
        for(Tuple2<Long,String> tuple2:top3categorySessionCountRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //23------24b105d40fe04d5cbff91d48ac34e76b,1
        //7------24b105d40fe04d5cbff91d48ac34e76b,1
        //67------17fbe9a8cb2a400194c4c877a8fef467,1

        //将top10的品类与上面计算的RDD相join 得到每一个热门品类的点击次数
        JavaPairRDD<Long,String> top10CategorySessionCountRDD = top10CategoryIdsRDD.join(categorySessionCountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> longTuple2Tuple2) throws Exception {
                return new Tuple2<>(longTuple2Tuple2._1,longTuple2Tuple2._2._2);
            }
        });

        // 根据品类分组
        JavaPairRDD<Long,Iterable<String>> top10CategorySessionCountGroupRDD = top10CategorySessionCountRDD.groupByKey();
        // 查看
        System.out.println("top10CategorySessionCountGroupRDD-1515151515151515151515");
        List<Tuple2<Long,Iterable<String>>> top3top10CategorySessionCountGroupRDD = top10CategorySessionCountGroupRDD.take(3);
        for(Tuple2<Long,Iterable<String>> tuple2:top3top10CategorySessionCountGroupRDD){
            System.out.print(tuple2._1+"------");
            System.out.print(tuple2._2);
            System.out.println();
        }
        //39------[c8064e3bdcff429eafa16af4883b090c,1, 0a2699df3a494032bb8330a88682f141,1, f5e2f97afabf4f2e84c0108cd11487e6,1, 4f27ea261310434eb19e098a05b372ac,1, cc9dcd52017c4c7db6c1d863188ea67e,1, a0f9365273bf40f0876284087d58df33,1, 68350d887f6747299eeb553294333d0a,1, bceb370e73484c5ea427cd1a3f3d3ebc,1, 695f2130c52040d09f3ec79bffb463b7,1, 6181b0509a7041329a96e9d7987d08a4,1, 0fca2b52fdb347328919e9e14c55381f,1, c6f4a8a690774435b099a6755f547b26,1, 4217b120b39a410b8b8d32e33b4244f1,1, d00f206d9c834e86bf4241fbbeae0407,1, 0b0b41515cac4c94bdebaa65584b191b,1, a77faa50e1ad46a686a50806db047694,1, e914bff6403e4514aef75f9c71dade80,1, f99a8785aca84f69bc643c2800ab8efe,1, 2b180073434c4e9da8a0876ec9b656f4,1, 364b00b31d0e4a5e8eb66a6e41c1a20f,1, 72f952219f724f98a7e05d0255aa5848,1, 186222ad6219411cbcd004a16b0fc4c9,1, 26e4bdda3696447d8fc02ff3fd23750b,1, 59ee2337b2874784ad53404fdcda28ad,1, 4caef8285cab4370a52264b786f0cdfa,1, f211a1b566e74f13886308e74074b397,1, 86d83e9a586340f7b296d126ccbfa0be,1, f8eb12afe31343d1bbb30d73e54a52e8,1, 272b7a5997804c7faebed2e446bb64b0,1, f76210a7ec864f5a8d47758d729daf62,1]

        JavaPairRDD<String,String> top10CategorySessionRDD = top10CategorySessionCountGroupRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> longIterableTuple2) throws Exception {
                List<Top10CategorySession> top10CategorySessionList=new ArrayList<Top10CategorySession>();
                Long categoryId = longIterableTuple2._1;
                String[] top10Sessions = new String[10];
                List<Tuple2<String,String>> sessionIdList = new ArrayList<>();

                for(String sessionCount:longIterableTuple2._2){
                    String[] sessionCountSplited=sessionCount.split(" ");
                    Long count=Long.valueOf(sessionCountSplited[1]);

                    for(int i=0;i<top10Sessions.length;i++){
                        if(top10Sessions[i] == null){
                            top10Sessions[i] = sessionCount;
                        }else {
                            Long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
                            if(count>_count){
                                for(int j=9;j>i;j--){
                                    top10Sessions[j] = top10Sessions[j-1];
                                }
                                top10Sessions[i] = sessionCount;
                                break;
                            }
                        }
                    }

                }
                System.out.println(top10Sessions.toString());
                // 封装数据
                for(int i=0;i<top10Sessions.length;i++){
                    if(top10Sessions[i] != null){
                        Top10CategorySession top10CategorySession = new Top10CategorySession();
                        String sessionId=top10Sessions[i].split(",")[0];
                        Long count=Long.valueOf(top10Sessions[i].split(",")[1]);
                        top10CategorySession.set(taskId,categoryId,sessionId,count);
                        top10CategorySessionList.add(top10CategorySession);
                        sessionIdList.add(new Tuple2<String, String>(sessionId,sessionId));
                    }
                }
                //批量插入数据库
                DaoFactory.getTop10CategorySessionDao().batchInsert(top10CategorySessionList);
                return sessionIdList.iterator();

            }
        });





    }
}
