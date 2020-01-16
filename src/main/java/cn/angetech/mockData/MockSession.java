package cn.angetech.mockData;

import cn.angetech.util.DateUtils;
import cn.angetech.util.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MockSession {

    public static List<Row> mock(){
        // 生成当天的日期
        String date = DateUtils.getTodayDate();
        // 生成唯一的4个action
        String[] actions = new String[]{"search", "click", "order", "pay"};
        String[] searchKeywords = new String[] {"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        final Random random = new Random();
        List<Row> rowList = new ArrayList<>();

        // 模拟一个用户数据
        for(int i=0;i<10;i++){
            Long userid = 1L;
            String sessionid = "sessionid";
            Long pageid = 1L;
            String searchKeyword = null;
            Long clickCategoryId = null;
            Long clickProductId = null;
            String orderCategoryIds = null;
            String orderProductIds = null;
            String payCategoryIds = null;
            String payProductIds = null;
            String action = actions[random.nextInt(4)];
            String actionTime = DateUtils.getLocalTime();
            if("search".equals(action)){
                searchKeyword = searchKeywords[random.nextInt(10)];
            }else if("click".equals(action)){
                clickCategoryId = Long.valueOf(i);
                clickProductId = Long.valueOf(i);
            }else if("order".equals(action)){
                orderCategoryIds = "1,2,3";
                orderProductIds = "1,2,3";
            }else if("pay".equals(action)){
                payCategoryIds = "3,4,5";
                payProductIds = "3,4,5";
            }
            Row row = RowFactory.create(date,userid,sessionid,pageid,action,actionTime,searchKeyword,clickCategoryId,clickProductId,orderCategoryIds,orderProductIds,payCategoryIds,payProductIds);
            rowList.add(row);
        }

        return rowList;




    }





}
