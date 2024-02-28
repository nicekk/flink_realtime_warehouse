package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wangkai
 * @date 2024/2/28 16:51
 **/
public class DwdTradeCartAdd extends BaseSqlApp {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, "dwd_trade_cart_add");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupId) {
        createTopicDb(ckAndGroupId, tableEnv);

        tableEnv.executeSql("create table cart_add (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  cart_price string,\n" +
                "  sku_num bigint,\n" +
                "  img_url string,\n" +
                "  sku_name string,\n" +
                "  is_checked string,\n" +
                "  create_time string,\n" +
                "  operate_time string,\n" +
                "  is_ordered string,\n" +
                "  order_time string,\n" +
                "  ts bigint\n" +
                ")" + SqlUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD));

        tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id,\n" +
                "  `data`['user_id'] user_id,\n" +
                "  `data`['sku_id'] sku_id,\n" +
                "  `data`['cart_price'] cart_price,\n" +
                "  if ( `type` = 'insert',cast (`data`['sku_num'] as bigint),cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint)) as sku_num,\n" +
                "  `data`['img_url'] img_url,\n" +
                "  `data`['sku_name'] sku_name,\n" +
                "  `data`['is_checked'] is_checked,\n" +
                "  `data`['create_time'] create_time,\n" +
                "  `data`['operate_time'] operate_time,\n" +
                "  `data`['is_ordered'] is_ordered,\n" +
                "  `data`['order_time'] order_time,\n" +
                "  ts\n" +
                "  from topic_db\n" +
                " where `database` = 'gmall'\n" +
                "   and `table` = 'cart_info'\n" +
                "   and (\n" +
                "    `type` = 'insert' or \n" +
                "    (`type` = 'update' \n" +
                "      and `old`['sku_num'] is not null \n" +
                "      and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint)))")
                .insertInto("cart_add").execute();


    }
}
