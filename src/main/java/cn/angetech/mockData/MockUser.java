package cn.angetech.mockData;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MockUser {
    public static List<Row> mock(){
        final Random random = new Random();
        String[] sexes = new String[]{"male", "female"};
        List<Row> rowList = new ArrayList<>();
        for(int i = 0; i < 10; i ++) {
            long userid = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userid, username, name, age,
                    professional, city, sex);
            rowList.add(row);
        }
        return rowList;
    }
}
