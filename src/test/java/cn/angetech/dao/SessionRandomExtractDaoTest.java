package cn.angetech.dao;


import cn.angetech.dao.factory.DaoFactory;
import cn.angetech.domain.SessionRandomExtract;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SessionRandomExtractDaoTest {
    @Test
    public void testBatchInsert()
    {
        List<SessionRandomExtract> sessionRandomExtractList=new ArrayList<SessionRandomExtract>();
        SessionRandomExtract sessionRandomExtract1=new SessionRandomExtract();
        sessionRandomExtract1.set("1","2","3","4");
        SessionRandomExtract sessionRandomExtract2=new SessionRandomExtract();
        sessionRandomExtract2.set("1","2","3","4");

        sessionRandomExtractList.add(sessionRandomExtract1);
        sessionRandomExtractList.add(sessionRandomExtract2);
        DaoFactory.getSessionRandomExtractDao().batchInsert(sessionRandomExtractList);
    }
}
