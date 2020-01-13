package cn.angetech.dao.impl;

import cn.angetech.dao.SessionDetailDao;
import cn.angetech.domain.SessionDetail;
import cn.angetech.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class SessionDetailDaoImpl implements SessionDetailDao {
    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail (user_id,session_id,page_id,action_time,search_keyword,click_category_id,click_product_id,order_category_ids,order_product_ids,pay_category_ids,pay_product_ids) values(?,?,?,?,?,?,?,?,?,?,?)";
        Object[] object = new Object[]{

                sessionDetail.getUserId(),
                sessionDetail.getSessinId(), sessionDetail.getPageid(), sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyWord(), sessionDetail.getClickCategoryId(), sessionDetail.getClickProductId()
                , sessionDetail.getOrderCategoryIds(), sessionDetail.getOrderProductIds(), sessionDetail.getPayCategoryIds(), sessionDetail.getPayProductIds()
        };
        JDBCHelper.getInstance().excuteUpdate(sql,object);
    }

    @Override
    public void batchInsert(List<SessionDetail> sessionDetailList) {
        String sql = "insert into session_detail (user_id,session_id,page_id,action_time,search_keyword,click_category_id,click_product_id,order_category_ids,order_product_ids,pay_category_ids,pay_product_ids) values(?,?,?,?,?,?,?,?,?,?,?)";

        List<Object[]> paramList=new ArrayList<Object[]>();
        for (SessionDetail sessionDetail:sessionDetailList)
        {
            Object[] object=new  Object[]{sessionDetail.getUserId(),
                    sessionDetail.getSessinId(),sessionDetail.getPageid(),sessionDetail.getActionTime(),
                    sessionDetail.getSearchKeyWord(),sessionDetail.getClickCategoryId(),sessionDetail.getClickProductId()
                    ,sessionDetail.getOrderCategoryIds(),sessionDetail.getOrderProductIds(),sessionDetail.getPayCategoryIds(),sessionDetail.getPayProductIds()};
            paramList.add(object);
        }
        JDBCHelper.getInstance().excuteBatch(sql,paramList);
    }
}
