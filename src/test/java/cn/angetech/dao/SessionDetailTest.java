package cn.angetech.dao;


import cn.angetech.dao.factory.DaoFactory;
import cn.angetech.domain.SessionDetail;
import org.junit.Test;

public class SessionDetailTest {
    @Test
    public void testInsert()
    {
        SessionDetail sessionDetail=new SessionDetail();
        sessionDetail.set(1L,"1",1L,"1","1",1L,1L,"1","1","1","1");
        DaoFactory.getSessionDetailDao().insert(sessionDetail);
    }
}
