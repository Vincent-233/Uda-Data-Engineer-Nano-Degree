## 备忘
1. 开始工作前，运行一下“L3 Exercise 2 - 用Python创建AWS角色、RedShift集群等”
告一段落之后，记得将集群删除；
2. 每次重新运行之后，可能更新配置中关于redshift的Host和ARN；

3. S3文件，见此[链接](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/?region=us-west-2&tab=overview);

4. log_data和song_data只有一个文件夹，具有相同前缀，方便使用copy命令一次性加载，[copy命令加载json示例文档](https://docs.aws.amazon.com/zh_cn/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json)，其中可能会用到一个jsonpath，在路径udacity-dend/log_json_path.json下；

## 思路
1. 分别建立song和log的stage表，用于承接copy而来的命令，接着用纯sql执行ETL过程即可；

2. 维表和事实表的建表语句可参考P1;

3. 

## 其他备忘
- copy data 命令，一共花了45分钟，不过才10000多条数据
- 从 stage 到 user 表时，目前是用的简单的distinct，实际应该用row_number子查询，取最后一次的数据

