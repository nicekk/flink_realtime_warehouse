package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import com.atguigu.gmall.realtime.dws.function.KwSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSqlApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021,1,"dws_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupId) {
        tableEnv.executeSql("create table page_info (\n" +
                "  `common` map<String,String>,\n" +
                "  `page` map<String,String>,\n" +
                "  `ts` bigint,\n" +
                "  `row_time` as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "  WATERMARK FOR row_time as row_time - INTERVAL '5' SECOND\n" +
                ")" + SqlUtil.getKafkaDDLSource(ckAndGroupId, Constant.TOPIC_DWD_TRAFFIC_PAGE));

        Table keyWordsTable = tableEnv.sqlQuery("select page['item'] keywords,\n" +
                "       row_time\n" +
                "  from page_info\n" +
                " where page['last_page_id']= 'search'\n" +
                "   and page['item_type']= 'keyword'\n" +
                "   and page['item'] is not null");

        tableEnv.createTemporarySystemFunction("SplitFunction", KwSplit.class);

        tableEnv.createTemporaryView("keywords_table", keyWordsTable);

        Table keywordTable = tableEnv.sqlQuery("select keywords,keyword,`row_time`\n" +
                "  from keywords_table\n" +
                "  left join LATERAL TABLE(SplitFunction(keywords)) on true");

        tableEnv.createTemporaryView("keyword_table", keywordTable);

        // 分组开窗聚合
        Table resultTable = tableEnv.sqlQuery("select cast(TUMBLE_START(row_time, INTERVAL '10' SECOND) as STRING) as stt,\n" +
                "       CAST(TUMBLE_END(row_time, INTERVAL '10' SECOND) AS STRING) as edt,\n" +
                "       cast(CURRENT_DATE as string) cur_date,\n" +
                "      keyword,\n" +
                "       count(1) keyword_count\n" +
                "  from keyword_table\n" +
                " group by \n" +
                "  TUMBLE(row_time,INTERVAL '10' SECOND),\n" +
                "  keyword");

        tableEnv.executeSql("CREATE TABLE doris_sink (\n" +
                "    stt STRING,\n" +
                "    edt STRING,\n" +
                "    cur_date STRING,\n" +
                "    keyword STRING,\n" +
                "    keyword_count bigint\n" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = 'hadoop:7030',\n" +
                "      'table.identifier' = 'gmall2023_realtime.dws_traffic_source_keyword_page_view_window',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '',\n" +
                "      'sink.label-prefix' = 'doris_label'\n" +
                ")");

        resultTable.insertInto("doris_sink").execute();

    }
}
