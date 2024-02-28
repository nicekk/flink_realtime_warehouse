package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

/**
 * @author wangkai
 * @date 2024/2/28 15:59
 **/
public class SqlUtil {

    public static String getKafkaDDLSource(String groupId, String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'json.ignore-parse-errors' = 'true'," + // 当 json 解析失败的时候,忽略这条数据
                "  'format' = 'json' " +
                ")";
    }

    public static String getKafkaTopicDb(String groupId) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `type` STRING,\n" +
                "  `data` MAP<STRING,STRING>,\n" +
                "  `old` MAP<STRING,STRING>,\n" +
                "  `proc_time` as PROCTIME()\n" +
                ")" + getKafkaDDLSource(groupId, Constant.TOPIC_DB);
    }

    public static String getKafkaDDLSink(String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'format' = 'json' " +
                ")";
    }
}
