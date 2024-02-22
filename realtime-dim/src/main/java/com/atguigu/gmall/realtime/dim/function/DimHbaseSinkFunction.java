package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

@Slf4j
public class DimHbaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {


    private Connection hBaseConnection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseConnection     = HBaseUtil.getHBaseConnection();

    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObject = value.f0;
        JSONObject data = jsonObject.getJSONObject("data");

        // insert update delete bootstrap-insert
        String type = jsonObject.getString("type");
        if ("delete".equals(type)) {
            // 删除对应的维度表数据
            delete(data, value.f1);
        } else {
            // 覆盖写入维度表数据
            put(data, value.f1);
        }

    }

    private void put(JSONObject data, TableProcessDim f1) {
        String rowkeyName = f1.getSinkRowKey();
        String rowKey = data.getString(rowkeyName);
        try {
            HBaseUtil.putRow(hBaseConnection, Constant.HBASE_NAMESPACE, f1.getSinkTable(), rowKey, f1.getSinkFamily(), data);
        } catch (IOException e) {
            log.error("写数据异常");
        }
    }

    private void delete(JSONObject data, TableProcessDim f1) {
        String rowkeyName = f1.getSinkRowKey();
        String rowKey = data.getString(rowkeyName);
        try {
            HBaseUtil.delRow(hBaseConnection,Constant.HBASE_NAMESPACE,f1.getSinkTable(),rowKey);
        } catch (IOException e) {
            log.error("删除数据异常");
        }
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(hBaseConnection);
    }
}
