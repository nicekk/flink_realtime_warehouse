package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wangkai
 * @date 2024/2/28 15:53
 **/
public class DwdInteractionCommentInfo extends BaseSqlApp {

    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,"dwd_interaction_comment_info");

    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv,String ckAndGroupId) {
        createTopicDb(ckAndGroupId, tableEnv);

        createBaseDic(tableEnv);

        // 清洗 topic_db，读出新增的数据
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
                "  ts,\n" +
                "  proc_time\n" +
                "  from topic_db\n" +
                " where `database` = 'gmall'\n" +
                "   and `table` = 'comment_info'\n" +
                "   and `type` = 'insert'");

        tableEnv.createTemporaryView("comment_info", commentInfoTable);

        // 使用 lookup join 完成维度表的关联
        Table joinTable = tableEnv.sqlQuery("select \n" +
                "  `id`,\n" +
                "  `user_id`,\n" +
                "  `sku_id`,\n" +
                "  `appraise` appraise_code,\n" +
                "  info.dic_name appraise_name,\n" +
                "  `comment_txt`,\n" +
                "  `ts`\n" +
                "  from comment_info c\n" +
                "  join base_dic FOR SYSTEM_TIME AS OF c.proc_time as b\n" +
                "    on c.appraise = b.rowkey");

        // 创建 kafka sink 表
        tableEnv.executeSql("create table dwd_interaction_comment_info(" +
                "id string, " +
                "user_id string," +
                "sku_id string," +
                "appraise string," +
                "appraise_name string," +
                "comment_txt string," +
                "ts bigint " +
                ")" + SqlUtil.getKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));


        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();
    }
}
