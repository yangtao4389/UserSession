package cn.angetech.dao.factory;

import cn.angetech.dao.*;
import cn.angetech.dao.impl.*;

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

    public static Top10CategoryDao getTop10CategoryDao(){return new Top10CategoryDaoImpl(); }

    public static Top10CategorySessionDao getTop10CategorySessionDao(){return new Top10CategorySessionDaoImpl(); }

    public static SessionAggrStatDao getSessionAggrStatDao(){return new SessionAggrStatDaoImpl(); }

}
