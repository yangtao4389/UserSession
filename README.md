# 用户session分析平台

## 编码思路
### 1.模拟数据
* mockData-> MockData
* 先创建hive表  
&nbsp;参考：https://blog.csdn.net/wuxintdrh/article/details/80903948
* 编写代码


### 模块解释
#### mockData
只有MockData是正式使用，其余的都是参考官方文档http://spark.apache.org/docs/latest/sql-getting-started.html#programmatically-specifying-the-schema

#### session
* 用户session分析
* UserVisitAnalyze 用户访问分析  主程序，直接运行

#### domain
* 放类似表的 实体类型

#### dao
* sql链接执行
