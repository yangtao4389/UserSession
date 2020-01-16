package cn.angetech.dao;



import cn.angetech.domain.SessionAggrStat;

import java.io.Serializable;
import java.util.List;

public interface SessionAggrStatDao extends Serializable{
    void insert(SessionAggrStat sessionAggrStat);
    void batchInsert(List<SessionAggrStat> sessionAggrStatList);
}
