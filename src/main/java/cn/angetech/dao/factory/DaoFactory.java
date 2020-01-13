package cn.angetech.dao.factory;

import cn.angetech.dao.SessionDetailDao;
import cn.angetech.dao.SessionRandomExtractDao;
import cn.angetech.dao.TaskDao;
import cn.angetech.dao.impl.SessionDetailDaoImpl;
import cn.angetech.dao.impl.SessionRandomExtractDaoImpl;
import cn.angetech.dao.impl.TaskDaoImpl;

public class DaoFactory {
    /*
    * 使用工厂模式
    * 生成几个数据接口
    * */
    public static TaskDao getTaskDao(){return new TaskDaoImpl();}

    public static SessionRandomExtractDao getSessionRandomExtractDao(){
        return new SessionRandomExtractDaoImpl();
    }

    public static SessionDetailDao getSessionDetailDao(){
        return new SessionDetailDaoImpl();
    }

}
