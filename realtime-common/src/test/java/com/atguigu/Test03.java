package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author wangkai
 * @date 2024/2/28 10:34
 **/
public class Test03 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10L));

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `type` STRING,\n" +
                "  `data` MAP<STRING,STRING>,\n" +
                "  `old` MAP<STRING,STRING>,\n" +
                "  `proc_time` as PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        TableResult tableResult = tableEnv.executeSql("select * from topic_db where `database` = 'gmall' and `table` = 'comment_info' ");
//        tableResult.print();

        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "  dic_code STRING,\n" +
                "  dic_name STRING,\n" +
                "  parent_code STRING,\n" +
                "  create_time TIMESTAMP,\n" +
                "  operate_time TIMESTAMP,\n" +
                "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "   'username'='root',\n" +
                "   'password'='000000',\n" +
                "   'table-name' = 'base_dic'\n" +
                ")");

        Table commentInfoTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id,\n" +
                "  `data`['user_id'] user_id,\n" +
                "  `data`['nick_name'] nick_name,\n" +
                "  `data`['head_img'] head_img,\n" +
                "  `data`['sku_id'] sku_id,\n" +
                "  `data`['spu_id'] spu_id,\n" +
                "  `data`['order_id'] order_id,\n" +
                "  `data`['appraise'] appraise,\n" +
                "  `data`['comment_txt'] comment_txt,\n" +
                "  `data`['create_time'] create_time,\n" +
                "  `data`['operate_time'] operate_time,\n" +
                "  proc_time\n" +
                "  from topic_db\n" +
                " where `database` = 'gmall'\n" +
                "   and `table` = 'comment_info'\n" +
                "   and `type` = 'insert'");

        tableEnv.createTemporaryView("comment_info",commentInfoTable);

        tableEnv.executeSql("select \n" +
                "  `id`,\n" +
                "  `user_id`,\n" +
                "  `nick_name`,\n" +
                "  `head_img`,\n" +
                "  `sku_id`,\n" +
                "  `spu_id`,\n" +
                "  `order_id`,\n" +
                "  `appraise` appraise_code,\n" +
                "  b.dic_name appraise_name,\n" +
                "  `comment_txt`,\n" +
                "  c.`create_time`,\n" +
                "  c.`operate_time`\n" +
                "  from comment_info c\n" +
                "  join base_dic FOR SYSTEM_TIME AS OF c.proc_time as b\n" +
                "    on c.appraise = b.dic_code").print();
    }
}
