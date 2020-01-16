package cn.angetech.mockData;

import cn.angetech.util.DateUtils;
import cn.angetech.util.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

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

        // 模拟10个用户数据（1-10），每个用户随机2条session  每个session 随机10个动作
        for(int i=1;i<=10;i++){
            Long userid = Long.valueOf(i);

            for(int j=0;j<2;j++){
                String sessionid = UUID.randomUUID().toString().replace("-","");
                String baseActionTime = date + " " + StringUtils.fulfuill(String.valueOf(random.nextInt(23)));
                for(int k=-1;k<random.nextInt(10);k++){
                    Long pageid = Long.valueOf(random.nextInt(10));
                    String searchKeyword = null;
                    Long clickCategoryId = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;
                    String action = actions[random.nextInt(4)];
                    String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))+ ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
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
            }



        }

        return rowList;




    }





}
