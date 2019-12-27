package cn.angetech.session;

import cn.angetech.dao.TaskDao;
import cn.angetech.dao.factory.DaoFactory;
import cn.angetech.domain.Task;
import cn.angetech.mockData.MockData;
import cn.angetech.util.ParamUtils;
import com.alibaba.fastjson.JSONObject;

public class UserVisitAnalyze {
    public static void main(String[] args) {
        // 生成模拟数据
        MockData.mock();

        //获取请求的taskid, 从数据库中查询到请求的参数
        TaskDao dao = DaoFactory.getTaskDao();
        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        // 从数据库中查询出相应的task
        Task task = dao.findTaskById(taskId);
        JSONObject jsonObject = JSONObject.parseObject(task.getTaskParam());

        // 开始写聚合


    }
}
