package cn.angetech.dao.factory;

import cn.angetech.dao.TaskDao;
import cn.angetech.dao.impl.TaskDaoImpl;

public class DaoFactory {
    /*
    * 使用工厂模式
    * 生成几个数据接口
    * */
    public static TaskDao getTaskDao(){return new TaskDaoImpl();}


}
