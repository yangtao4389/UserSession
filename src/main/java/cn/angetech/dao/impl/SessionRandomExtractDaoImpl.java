package cn.angetech.dao.impl;

import cn.angetech.dao.SessionRandomExtractDao;
import cn.angetech.domain.SessionRandomExtract;
import cn.angetech.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class SessionRandomExtractDaoImpl implements SessionRandomExtractDao{
    @Override
    public void batchInsert(List<SessionRandomExtract> sessionRandomExtractList) {
        String sql = "insert into session_random_extract (session_id,start_time,search_keywords,click_category_ids) values(?,?,?,?)";
        List<Object[]> paramList = new ArrayList<>();
        for(SessionRandomExtract sessionRandomExtract:sessionRandomExtractList){
            Object[] params = new Object[]{sessionRandomExtract.getSessionId()
                    ,sessionRandomExtract.getStartTime(),sessionRandomExtract.getSearchKeyWords(),sessionRandomExtract.getClick_category_ids()};
            paramList.add(params);

        }
        JDBCHelper.getInstance().excuteBatch(sql,paramList);
    }
}
