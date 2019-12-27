package cn.angetech.dao;

import cn.angetech.domain.Task;

public interface TaskDao {
    Task findTaskById(Long id);
}
