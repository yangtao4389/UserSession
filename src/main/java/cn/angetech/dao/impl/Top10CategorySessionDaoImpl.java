package cn.angetech.dao.impl;


import cn.angetech.dao.Top10CategorySessionDao;
import cn.angetech.domain.Top10CategorySession;
import cn.angetech.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class Top10CategorySessionDaoImpl implements Top10CategorySessionDao
{
    @Override
    public void batchInsert(List<Top10CategorySession> top10CategorySessionList) {
        String sql="insert into top10_category_session (category_id,session_id,click_count) values(?,?,?)";
        List<Object[]> paramList=new ArrayList<Object[]>();
        for(Top10CategorySession top10CategorySession:top10CategorySessionList)
        {
            Object[] param=new Object[]{top10CategorySession.getCategoryId(),
            top10CategorySession.getSessionId(),top10CategorySession.getClickCount()};
            paramList.add(param);
        }
        JDBCHelper.getInstance().excuteBatch(sql,paramList);
    }
}
