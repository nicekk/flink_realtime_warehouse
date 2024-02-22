package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

@Slf4j
public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {


    private HashMap<String, TableProcessDim> hashMap = new HashMap<>();

    private MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 在这里使用 jdbc 预先加载维度表数据
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(
                mysqlConnection, "select * from gmall2023_config.table_process_dim", TableProcessDim.class, true);

        for (TableProcessDim dim : tableProcessDims) {
            dim.setOp("r");
            hashMap.put(dim.getSourceTable(), dim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    // 处理广播流的数据
    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 读取广播状态，将配置表信息作为一个维度表的标记，写到广播状态

        BroadcastState<String, TableProcessDim> tableProcessState = context.getBroadcastState(broadcastState);
        String op = tableProcessDim.getOp();
        if ("d".equals(op)) {
            tableProcessState.remove(tableProcessDim.getSourceTable());
            // 同步删除 hashMap 中初始化加载的值
            hashMap.remove(tableProcessDim.getSourceTable());
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

        // 如果数据还未过来，状态为空
        if (tableProcessDim == null) {
            tableProcessDim = hashMap.get(tableName);
        }

        if (tableProcessDim != null) {
            collector.collect(Tuple2.of(value, tableProcessDim));
        }
    }
}
