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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class UserVisitAnalyze2 {
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

        // (session,"row")
        JavaPairRDD<String,Row> sessionRangeDateJavaPairRDD  = transJavaRDDToJavaPairRDD(sessionRangeDateJavaRDD);
        //todo 4 聚合session，得到session的访问步长，时间。
        JavaPairRDD<String,String> aggregateBySessionIdJavaPairRDD = aggregateBySessionId(sessionRangeDateJavaPairRDD);
        // sessionid------userId=1|sessionId=sessionid|pageIds=1,|actions=2020-01-16 16:37:54,|searchKeywords=重庆辣子鸡|clickCategoryIds=1,2,4,6,8|clickProductIds=1,2,4,6,8,|orderCategoryIds=1,2,3,|orderProductIds=1,2,3,|payCategoryIds=3,4,5,|payProductIds=3,4,5,|visitLength=0|stepLength=10|startTime=2020-01-16 16:37:54|endTime=2020-01-16 16:37:54
//        System.out.println("444444444444444-111111111");
//        List<Tuple2<String,String>> top3aggregateBySessionIdJavaPairRDD = aggregateBySessionIdJavaPairRDD.take(30);
//        for(Tuple2<String,String> tuple2:top3aggregateBySessionIdJavaPairRDD){
//            System.out.print(tuple2._1+"------");
//            System.out.print(tuple2._2);
//            System.out.println();
//        }

        //todo 5.记录sessionAggrStatAccumulator
        SessionAggrStatAccumulator sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
        ss.sparkContext().register(sessionAggrStatAccumulator,"AccumulatorV2-2:");
        addSessionAggrStatAccumulator(aggregateBySessionIdJavaPairRDD,sessionAggrStatAccumulator);

        //todo 6.查看sessionAggrStatAccumulator 并写入到数据库
        String s = sessionAggrStatAccumulator.value(); // session_count=20|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=1|1m_3m=0|3m_10m=0|10m_30m=10|30m=7|1_3=9|4_6=9|7_9=2|10_30=0|30_60=0|60=0
        System.out.println(s);
        insertIntoMysqlsessionAggrStat(s,taskId);


        //todo 6.top10的类目（最多点击>最多下单>最多付款）

    }
    private static JavaRDD<Row>  getActionRDD(SparkSession ss,JSONObject jsonObject){
        String startDate = ParamUtils.getParam(jsonObject, Constants.PARAM_STARTDATE);
        String endDate = ParamUtils.getParam(jsonObject, Constants.PARAM_ENDTDATE);
        Dataset<Row> dataset = ss.sql("SELECT * FROM user_visit_action where where date >= '"+startDate+"' and date <= '"+endDate+"'");
        dataset.show();
        return dataset.javaRDD();
    }
    private static JavaPairRDD<String,Row> transJavaRDDToJavaPairRDD(JavaRDD<Row> javaRDD){
        return javaRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2),row);
            }
        });

    }
    private static JavaPairRDD<String,String> aggregateBySessionId( JavaPairRDD<String,Row> javaPairRDD){
        JavaPairRDD<String,Iterable<Row>> iterableJavaPairRDD = javaPairRDD.groupByKey();

        // (session,"session集合String")
        JavaPairRDD<String,String> stringJavaPairRDD = iterableJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String sessionId = stringIterableTuple2._1;
                Iterable<Row> rows = stringIterableTuple2._2;

                StringBuffer pageIds = new StringBuffer();
                StringBuffer actions = new StringBuffer();
                StringBuffer searchKeyWords = new StringBuffer();
                StringBuffer clickCategoryIds = new StringBuffer();
                StringBuffer clickProductIds = new StringBuffer();
                StringBuffer orderCategoryIds = new StringBuffer();
                StringBuffer orderProductIds = new StringBuffer();
                StringBuffer payCategoryIds = new StringBuffer();
                StringBuffer payProductIds = new StringBuffer();
                Long userId = null;
                Date startTime = null;
                Date endTime = null;

                int stepLength = 0;

                for(Row row:rows){
                    userId = row.getLong(1);
                    if(!row.isNullAt(3)){
                        String s = String.valueOf(row.getLong(3));
                        if(!pageIds.toString().contains(s)){
                            pageIds.append(s+",");
                        }
                    }
                    if(!row.isNullAt(4)){
                        String s = row.getString(4);
                        if(!actions.toString().contains(s)){
                            actions.append(s+",");
                        }
                    }

                    // searchKeyword
                    if(!row.isNullAt(6)){
                        String s = row.getString(6);
                        if(!searchKeyWords.toString().contains(s)){
                            searchKeyWords.append(s+",");
                        }
                    }
                    if(!row.isNullAt(7)){
                        String s = String.valueOf(row.getLong(7));
                        if(!clickCategoryIds.toString().contains(s)){
                            clickCategoryIds.append(s+",");
                        }
                    }
                    if(!row.isNullAt(8)){
                        String s = String.valueOf(row.getLong(8));
                        if(!clickProductIds.toString().contains(s)){
                            clickProductIds.append(s+",");
                        }
                    }
                    if(!row.isNullAt(9)){
                        String s = row.getString(9);
                        String[] stringList = s.split(",");
                        for(String s1:stringList){
                            if(!orderCategoryIds.toString().contains(s1)){
                                orderCategoryIds.append(s1+",");
                            }
                        }

                    }
                    if(!row.isNullAt(10)){
                        String s = row.getString(10);
                        String[] stringList = s.split(",");
                        for(String s1:stringList){
                            if(!orderProductIds.toString().contains(s1)){
                                orderProductIds.append(s1+",");
                            }
                        }

                    }
                    if(!row.isNullAt(11)){
                        String s = row.getString(11);
                        String[] stringList = s.split(",");
                        for(String s1:stringList){
                            if(!payCategoryIds.toString().contains(s1)){
                                payCategoryIds.append(s1+",");
                            }
                        }

                    }
                    if(!row.isNullAt(12)){
                        String s = row.getString(12);
                        String[] stringList = s.split(",");
                        for(String s1:stringList){
                            if(!payProductIds.toString().contains(s1)){
                                payProductIds.append(s1+",");
                            }
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
                String searchKeywordsInfo= StringUtils.trimComma(searchKeyWords.toString());
                String clickCategoryIdsInfo=StringUtils.trimComma(clickCategoryIds.toString());



                String info =
                        Constants.FIELD_USERID+"="+userId+"|"
                        +Constants.FIELD_SESSIONID+"="+sessionId+"|"
                        +Constants.FIELD_PAGEIDS+"="+pageIds+"|"
                        +Constants.FIELD_ACTIONS+"="+actions+"|"
                        +Constants.FIELD_SERACH_KEYWORDS+"="+searchKeywordsInfo+"|"
                        +Constants.FIELD_CLICK_CATEGORYIDS+"="+clickCategoryIdsInfo+"|"
                        +Constants.FIELD_CLICK_PRODUCTIDS+"="+clickProductIds+"|"
                        +Constants.FIELD_ORDER_CATEGORYIDS+"="+orderCategoryIds+"|"
                        +Constants.FIELD_ORDER_PRODUCTIDS+"="+orderProductIds+"|"
                        +Constants.FIELD_PAY_CATEGORYIDS+"="+payCategoryIds+"|"
                        +Constants.FIELD_PAY_PRODUCTIDS+"="+payProductIds+"|"
                        +Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"
                        +Constants.FIELD_STEP_LENGTH+"="+stepLength+"|"
                        +Constants.FIELD_START_TIME+"="+DateUtils.formatTime(startTime)+"|"
                        +Constants.FIELD_END_TIME+"="+DateUtils.formatTime(endTime);
                return new Tuple2<String, String>(sessionId,info);

            }


        });

        return stringJavaPairRDD;

    }

    private static void addSessionAggrStatAccumulator(JavaPairRDD<String,String> javaPairRDD,SessionAggrStatAccumulator sessionAggrStatAccumulator){
        javaPairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String sessionInfo = stringStringTuple2._2;
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                Long visitLength=Long.valueOf(StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.FIELD_VISIT_LENGTH));
                Integer stepLength=Integer.valueOf(StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.FIELD_STEP_LENGTH));
                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);
            }
            private void calculateVisitLength(Long visitLegth){
                if(visitLegth<1)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_0s);
                else if(visitLegth>=1&&visitLegth<=3)
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

            private void calculateStepLength(int stepLength){
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

    }

    private static void filterSessionAndAggrStat(JavaPairRDD<String,String> javaPairRDD){

    }


    private static void insertIntoMysqlsessionAggrStat(String value,Long taskId){

        Long sessionCount=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.SESSION_COUNT));
        //各个范围的访问时长
        Double visit_Length_0s=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_0s));
        Double visit_Length_1s_3s=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1s_3s));
        Double visit_Length_4s_6s=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_4s_6s));
        Double visit_Length_7s_9s=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_7s_9s));
        Double visit_Length_10s_30s=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10s_30s));
        Double visit_Length_30s_60s=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30s_60s));
        Double visit_Length_1m_3m=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1m_3m));
        Double visit_Length_3m_10m=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_3m_10m));
        Double visit_Length_10m_30m=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10m_30m));
        Double visit_Length_30m=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30m));

        //各个范围的访问步长
        Double step_Length_1_3=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_1_3));
        Double step_Length_4_6=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_4_6));
        Double step_Length_7_9=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_7_9));
        Double step_Length_10_30=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_10_30));
        Double step_Length_30_60=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_30_60));
        Double step_Length_60=Double.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_60));

        //访问时长对应的sesison占比，保留3位小数
        double visit_Length_0s_ratio= NumberUtils.formatDouble(visit_Length_0s/sessionCount,3);
        double visit_Length_1s_3s_ratio= NumberUtils.formatDouble(visit_Length_1s_3s/sessionCount,3);
        double visit_Length_4s_6s_ratio=NumberUtils.formatDouble(visit_Length_4s_6s/sessionCount,3);
        double visit_Length_7s_9s_ratio=NumberUtils.formatDouble(visit_Length_7s_9s/sessionCount,3);
        double visit_Length_10s_30s_ratio=NumberUtils.formatDouble(visit_Length_10s_30s/sessionCount,3);
        double visit_Length_30s_60s_ratio=NumberUtils.formatDouble(visit_Length_30s_60s/sessionCount,3);
        double visit_Length_1m_3m_ratio=NumberUtils.formatDouble(visit_Length_1m_3m/sessionCount,3);
        double visit_Length_3m_10m_ratio=NumberUtils.formatDouble(visit_Length_3m_10m/sessionCount,3);
        double visit_Length_10m_30m_ratio=NumberUtils.formatDouble(visit_Length_10m_30m/sessionCount,3);
        double visit_Length_30m_ratio=NumberUtils.formatDouble(visit_Length_30m/sessionCount,3);

        //访问步长对应的session占比，保留3位小数
        double step_Length_1_3_ratio= NumberUtils.formatDouble(step_Length_1_3/sessionCount,3);
        double step_Length_4_6_ratio=NumberUtils.formatDouble(step_Length_4_6/sessionCount,3);
        double step_Length_7_9_ratio=NumberUtils.formatDouble(step_Length_7_9/sessionCount,3);
        double c=NumberUtils.formatDouble(step_Length_10_30/sessionCount,3);
        double step_Length_30_60_ratio=NumberUtils.formatDouble(step_Length_30_60/sessionCount,3);
        double step_Length_60_ratio=NumberUtils.formatDouble(step_Length_60/sessionCount,3);

        SessionAggrStat sessionAggrStat=new SessionAggrStat();
        sessionAggrStat.set(taskId,sessionCount,visit_Length_0s_ratio,visit_Length_1s_3s_ratio,visit_Length_4s_6s_ratio,
                visit_Length_7s_9s_ratio,visit_Length_10s_30s_ratio,visit_Length_30s_60s_ratio,
                visit_Length_1m_3m_ratio,visit_Length_3m_10m_ratio,visit_Length_10m_30m_ratio,visit_Length_30m_ratio
                ,step_Length_1_3_ratio,step_Length_4_6_ratio,step_Length_7_9_ratio,step_Length_7_9_ratio,step_Length_30_60_ratio,step_Length_60_ratio);
//        List<SessionAggrStat> sessionAggrStatList=new ArrayList<SessionAggrStat>();
//        sessionAggrStatList.add(sessionAggrStat);
        // 插入数据库
//        DaoFactory.getSessionAggrStatDao().batchInsert(sessionAggrStatList);
        DaoFactory.getSessionAggrStatDao().insert(sessionAggrStat);
    }



}
