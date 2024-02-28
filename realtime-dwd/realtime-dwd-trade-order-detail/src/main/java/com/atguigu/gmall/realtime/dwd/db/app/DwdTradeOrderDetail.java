package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014, 4, "dwd_trade_order_detail");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupId) {

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        createTopicDb(ckAndGroupId, tableEnv);

        // 筛选 订单详情表数据
        Table orderDetailTable = tableEnv.sqlQuery("select \n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['sku_name'] sku_name,\n" +
                "\t`data`['order_price'] order_price,\n" +
                "\t`data`['sku_num'] sku_num,\n" +
                "\t`data`['create_time'] create_time,\n" +
                "\t`data`['split_total_amount'] split_total_amount,\n" +
                "\t`data`['split_activity_amount'] split_activity_amount,\n" +
                "\t`data`['split_coupon_amount'] split_coupon_amount,\n" +
                "\tts\n" +
                "  from topic_db\n" +
                " where `database` = 'gmall'\n" +
                "   and `table` = 'order_detail'\n" +
                "   and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail", orderDetailTable);



        // 筛选 订单信息表

        Table orderInfoTable = tableEnv.sqlQuery("select \n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['province_id'] province_id,\n" +
                "\tts\n" +
                "  from topic_db\n" +
                " where `database` = 'gmall'\n" +
                "   and `table` = 'order_info'\n" +
                "   and `type` = 'insert'");

        tableEnv.createTemporaryView("order_info", orderInfoTable);




        // 筛选 订单详情活动关联表
        Table odaTable = tableEnv.sqlQuery("select \n" +
                "\t`data`['order_detail_id'] order_detail_id,\n" +
                "\t`data`['activity_id'] activity_id,\n" +
                "\t`data`['activity_rule_id'] activity_rule_id,\n" +
                "\tts\n" +
                "  from topic_db\n" +
                " where `database` = 'gmall'\n" +
                "   and `table` = 'order_detail_activity'\n" +
                "   and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail_activity", odaTable);

        // 筛选 订单详情优惠券关联表

        Table odcTable = tableEnv.sqlQuery("   select \n" +
                "\t`data`['order_detail_id'] order_detail_id,\n" +
                "\t`data`['coupon_id'] coupon_id,\n" +
                "\tts\n" +
                "  from topic_db\n" +
                " where `database` = 'gmall'\n" +
                "   and `table` = 'order_detail_coupon'\n" +
                "   and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail_coupon", odcTable);


        Table joinTable = tableEnv.sqlQuery(" select \n" +
                "\tod.id,\n" +
                "\torder_id,\n" +
                "\tsku_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tcreate_time,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tuser_id,\n" +
                "\tprovince_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id\n" +
                "   from order_detail od\n" +
                "   join order_info oi\n" +
                "     on od.order_id = oi.id\n" +
                "   left join order_detail_activity oda\n" +
                "     on od.id = oda.order_detail_id\n" +
                "   left join order_detail_coupon odc\n" +
                "     on od.id = odc.order_detail_id");

        // 使用了 left join，必然会产生 撤回流，此时就需要用到 upsert-kafka
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + "(\n" +
                "id STRING,\n" +
                "order_id STRING,\n" +
                "sku_id STRING,\n" +
                "sku_name STRING,\n" +
                "order_price STRING,\n" +
                "sku_num STRING,\n" +
                "create_time STRING,\n" +
                "split_total_amount STRING,\n" +
                "split_activity_amount STRING,\n" +
                "split_coupon_amount STRING,\n" +
                "user_id STRING,\n" +
                "province_id STRING,\n" +
                "activity_id STRING,\n" +
                "activity_rule_id STRING,\n" +
                "coupon_id STRING,\n" +
                "PRIMARY KEY(id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();


    }
}
