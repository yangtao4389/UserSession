# 用户session分析平台

# 项目目的
提取用户访问的session数据，对session进行统计，最后把数据写入数据库


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
* CollectionAccumulator、CustomAccumulatorExample ：练习如何使用spark 累加器 
* Transformation1-12 练习来源： https://www.cnblogs.com/Transkai/p/11346639.html


#### domain
* 放类似表的 实体类型

#### dao
* sql链接执行

#### accumulatorTest
测试accumulatorV2
参考：https://blog.csdn.net/hlp4207/article/details/85287788
实现累加效果：ONE:1    TWO:2    THREE:3    ONE:9
UserDefinedAccumulator 
