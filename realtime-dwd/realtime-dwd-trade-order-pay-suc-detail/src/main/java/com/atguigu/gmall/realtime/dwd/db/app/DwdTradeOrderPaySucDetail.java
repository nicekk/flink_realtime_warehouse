package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, "dwd_trade_order_pay_suc_detail");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupId) {
        // 核心业务逻辑

        // 读取 topic_db 主题数据
        createTopicDb(ckAndGroupId, tableEnv);

        // 筛选出支付成功数据（当状态从1601->1602的时候，就是支付成功了）
        Table paymentTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id,\n" +
                "  `data`['order_id'] order_id,\n" +
                "  `data`['user_id'] user_id,\n" +
                "  `data`['payment_type'] payment_type,\n" +
                "  `data`['total_amount'] total_amount,\n" +
                "  `data`['callback_time'] callback_time,\n" +
                "  ts,\n" +
                "  row_time,\n" +
                "  proc_time\n" +
                "  from topic_db\n" +
                " where `database` = 'gmall'\n" +
                "   and `table` = 'payment_info'\n" +
                "   and `type` = 'update'\n" +
                "   and `old`['payment_status'] is not null\n" +
                "   and `data`['payment_status'] = '1602'");

        tableEnv.createTemporaryView("payment", paymentTable);

        // 读取下单详情表数据
        tableEnv.executeSql("create table order_detail (\n" +
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
                "ts BIGINT,\n" +
                "row_time as TO_TIMESTAMP_LTZ(ts * 1000,3),\n" +
                "WATERMARK FOR row_time as row_time - INTERVAL '5' SECOND\n" +
                ")" + SqlUtil.getKafkaDDLSource( ckAndGroupId,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        // 创建 base_dic 字典表
        createBaseDic(tableEnv);

        // 使用 interval join 完成数据
        Table payOrderTable = tableEnv.sqlQuery("select \n" +
                "  od.id,\n" +
                "  p.order_id,\n" +
                "  p.user_id,\n" +
                "  payment_type,\n" +
                "  callback_time,\n" +
                "  sku_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  p.ts,\n" +
                "  p.proc_time\n" +
                " FROM payment p join order_detail od\n" +
                "   on p.order_id = od.order_id\n" +
                "  and p.row_time between od.row_time - INTERVAL '15' MINUTE\n" +
                "  AND od.row_time + INTERVAL '5' SECOND");

        tableEnv.createTemporaryView("pay_order", payOrderTable);

        // 使用 lookup-join 完成维度退化

        Table payOrderDicTable = tableEnv.sqlQuery("select \n" +
                "  id,\n" +
                "  order_id,\n" +
                "  user_id,\n" +
                "  payment_type payment_type_code,\n" +
                "  info.dic_name payment_type_name,\n" +
                "  callback_time,\n" +
                "  sku_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  ts\n" +
                " from pay_order p\n" +
                " left join base_dic FOR SYSTEM_TIME AS OF p.proc_time as b\n" +
                "   on p.payment_type = b.rowkey");

        // 创建 upsert kafka 写出
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + " (\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "callback_time string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "order_price string,\n" +
                "sku_num string,\n" +
                "split_total_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "ts bigint,\n" +
                "PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        payOrderDicTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();

    }
}
