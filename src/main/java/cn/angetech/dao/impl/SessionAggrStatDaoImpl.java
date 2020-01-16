package cn.angetech.dao.impl;



import cn.angetech.dao.SessionAggrStatDao;
import cn.angetech.domain.SessionAggrStat;
import cn.angetech.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class SessionAggrStatDaoImpl implements SessionAggrStatDao {
    @Override
    public void insert(SessionAggrStat sessionAggrStat) {
        String sql="insert into session_aggr_stat (task_id,session_count,0s,1s_3s,4s_6s,7s_9s,10s_30s,30s_60s,1m_3m,3m_10m,10m_30m,30m,1_3,4_6,7_9,10_30,30_60,60_) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        System.out.println(sql);
        Object[] params={sessionAggrStat.getTaskId(),sessionAggrStat.getSessionCount(),sessionAggrStat.getVisit_Length_0s(),sessionAggrStat.getVisit_Length_1s_3s(),
                sessionAggrStat.getVisit_Length_4s_6s(),sessionAggrStat.getVisit_Length_7s_9s(),
                sessionAggrStat.getVisit_Length_10s_30s(),sessionAggrStat.getVisit_Length_30s_60s(),
                sessionAggrStat.getVisit_Length_1m_3m(),sessionAggrStat.getVisit_Length_3m_10m()
                ,sessionAggrStat.getVisit_Length_10m_30m(),sessionAggrStat.getVisit_Length_30m(),
                sessionAggrStat.getStep_Length_1_3(),sessionAggrStat.getStep_Length_4_6(),sessionAggrStat.getStep_Length_7_9(),
                sessionAggrStat.getStep_Length_7_9(),sessionAggrStat.getStep_Length_10_30(),
                sessionAggrStat.getStep_Length_30_60()};
        JDBCHelper.getInstance().excuteUpdate(sql,params);
    }

    @Override
    public void batchInsert(List<SessionAggrStat> sessionAggrStatList) {
        String sql="insert into session_aggr_stat (task_id,session_count,0s,1s_3s,4s_6s,7s_9s,10s_30s,30s_60s,1m_3m,3m_10m,10m_30m,30m,1_3,4_6,7_9,10_30,30_60,60_) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        List<Object[]> paramList=new ArrayList<Object[]>();
        for (SessionAggrStat sessionAggrStat:sessionAggrStatList)
        {
            Object[] params={sessionAggrStat.getTaskId(),sessionAggrStat.getSessionCount(),sessionAggrStat.getVisit_Length_0s(),sessionAggrStat.getVisit_Length_1s_3s(),
                    sessionAggrStat.getVisit_Length_4s_6s(),sessionAggrStat.getVisit_Length_7s_9s(),
                    sessionAggrStat.getVisit_Length_10s_30s(),sessionAggrStat.getVisit_Length_30s_60s(),
                    sessionAggrStat.getVisit_Length_1m_3m(),sessionAggrStat.getVisit_Length_3m_10m()
                    ,sessionAggrStat.getVisit_Length_10m_30m(),sessionAggrStat.getVisit_Length_30m(),
                    sessionAggrStat.getStep_Length_1_3(),sessionAggrStat.getStep_Length_4_6(),sessionAggrStat.getStep_Length_7_9(),
                    sessionAggrStat.getStep_Length_7_9(),sessionAggrStat.getStep_Length_10_30(),
                    sessionAggrStat.getStep_Length_30_60()};
            paramList.add(params);
        }

        JDBCHelper.getInstance().excuteBatch(sql,paramList);
    }
}
