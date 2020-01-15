package cn.angetech.dao.impl;



import cn.angetech.dao.Top10CategoryDao;
import cn.angetech.domain.Top10Category;
import cn.angetech.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class Top10CategoryDaoImpl implements Top10CategoryDao {
    @Override
    public void insert(Top10Category top10Category) {
        String sql="insert into top10_category (category_id,click_count,order_count,pay_count) values(?,?,?,?)";
        Object[] params=new Object[]{
                top10Category.getCategoryId(),top10Category.getCategoryId(),
                top10Category.getClickCount(),top10Category.getOrderCount(),top10Category.getPayCount()
        };
        JDBCHelper.getInstance().excuteUpdate(sql,params);
    }

    @Override
    public void batchInsert(List<Top10Category> top10CategoryList) {
        String sql="insert into top10_category (category_id,click_count,order_count,pay_count) values(?,?,?,?)";
        List<Object[]> paramList=new ArrayList<Object[]>();
        for (Top10Category top10Category:top10CategoryList)
        {
            Object[] params=new Object[]{
                    top10Category.getCategoryId(),
                    top10Category.getClickCount(),top10Category.getOrderCount(),top10Category.getPayCount()
            };
            paramList.add(params);
        }

        JDBCHelper.getInstance().excuteBatch(sql,paramList);
    }
}
