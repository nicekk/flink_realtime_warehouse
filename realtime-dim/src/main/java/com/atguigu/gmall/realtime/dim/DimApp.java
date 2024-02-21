package com.atguigu.gmall.realtime.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

@Slf4j
public class DimApp extends BaseApp {

    public static void main(String[] args) {
        new DimApp().start(8087, 4, "dim_app", "topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> filterStream = etl(stream);

        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource("gmall2023_config", "table_process_dim");
        DataStreamSource<String> source1 = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);

        SingleOutputStreamOperator<TableProcessDim> createTableStream = createHbaseTable(source1);

        // 广播配置的流
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = createTableStream.broadcast(broadcastState);

        BroadcastConnectedStream<JSONObject, TableProcessDim> connectStream = filterStream.connect(broadcastStateStream);

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> streamResult = connectStream.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                // 在这里使用 jdbc 预先加载维度表数据

            }

            // 处理广播流的数据
            @Override
            public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                // 读取广播状态，将配置表信息作为一个维度表的标记，写到广播状态

                BroadcastState<String, TableProcessDim> tableProcessState = context.getBroadcastState(broadcastState);
                String op = tableProcessDim.getOp();
                if ("d".equals(op)) {
                    tableProcessState.remove(tableProcessDim.getSourceTable());
                } else {
                    tableProcessState.put(tableProcessDim.getSourceTable(), tableProcessDim);
                }

            }


            // 处理主流的数据
            @Override
            public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {

                // 读取广播状态，查询广播状态，判断当前的数据对应的表格是否存在于状态里
                ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = readOnlyContext.getBroadcastState(broadcastState);

                String tableName = value.getString("table");

                TableProcessDim tableProcessDim = tableProcessState.get(tableName);

                if (tableProcessDim != null) {
                    log.info("原始数据，是维度表：{}", value);
                    collector.collect(Tuple2.of(value, tableProcessDim));
                } else {
                    log.info("原始数据，不是维度表：{}", value);
                }
            }
        });

        streamResult.print();
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHbaseTable(DataStreamSource<String> source1) {
        return source1.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {

            private Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取到 hbase 的连接
                connection = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                // 关闭连接
                HBaseUtil.closeHBaseConn(connection);
            }

            @Override
            public void flatMap(String s, Collector<TableProcessDim> collector) throws Exception {
                // 使用读取到的配置表数据，到 hbase 创建与之关联的表
                log.info("flink cdc 读取到的原始数据：{}", s);
                try {
                    TableProcessDim tableProcessDim;
                    JSONObject jsonObject = JSON.parseObject(s);
                    String op = jsonObject.getString("op");
                    if ("d".equals(op)) {
                        tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        // 当配置表发送一条d类型，要删除这张维度表
                        deleteTable(tableProcessDim);
                        tableProcessDim.setOp(op);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        createTable(tableProcessDim);
                        tableProcessDim.setOp(op);
                    } else {
                        tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(tableProcessDim);
                        createTable(tableProcessDim);
                        tableProcessDim.setOp(op);
                    }
                    collector.collect(tableProcessDim);
                } catch (Exception e) {
                    log.error("读取 table_process_dim 失败", e);
                }


            }

            private void createTable(TableProcessDim tableProcessDim) {
                try {
                    HBaseUtil.createHBaseTable(connection, "gmall", tableProcessDim.getSinkTable(), tableProcessDim.getSinkFamily());
                } catch (IOException e) {
                    log.error("连接异常", e);
                    throw new RuntimeException(e);
                }
            }

            private void deleteTable(TableProcessDim tableProcessDim) {
                try {
                    HBaseUtil.dropHBaseTable(connection, "gmall", tableProcessDim.getSinkTable());
                } catch (IOException e) {
                    log.error("连接异常", e);
                    throw new RuntimeException(e);
                }

            }
        });
    }


    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject dataObj = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) && !"bootstrap-start".equals(type) &&
                            !"bootstrap-complete".equals(type) &&
                            dataObj != null && !dataObj.isEmpty()) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
