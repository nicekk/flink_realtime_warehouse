package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

@Slf4j
public class DwdBaseLog extends BaseApp {

    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4,"dwd_base_log","topic_log");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 过滤不完整的数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 2. 进行新旧访客修复
        KeyedStream<JSONObject, String> keyedStream = keyByWithWatermark(jsonObjStream);

        SingleOutputStreamOperator<JSONObject> isNewFixStream = fixIsNew(keyedStream);

        isNewFixStream.print();

    }

    private static SingleOutputStreamOperator<JSONObject> fixIsNew(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject commonObj = jsonObject.getJSONObject("common");
                String isNew = commonObj.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = jsonObject.getLong("ts");
                String currentDt = DateFormatUtil.tsToDate(ts);

                // 如果日志中 isNew 为 1，判断当前状态情况
                if ("1".equals(isNew)) {
                    if (firstLoginDt != null && !firstLoginDt.equals(currentDt)) {
                        commonObj.put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        firstLoginDtState.update(currentDt);
                    } else {
                        // 同一天，且是新访客。表示重复登录
                    }
                } else {
                    if (firstLoginDt == null) {
                        // 老用户，flink 实时数仓还未记录过这个访客，需要补充这个访客的信息
                        // 把这个访客的日期，记录成昨天
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        // 正常情况，不需要修复
                    }
                }

                collector.collect(jsonObject);
            }
        });


    }

    private static KeyedStream<JSONObject, String> keyByWithWatermark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })
        ).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    JSONObject pageObj = jsonObject.getJSONObject("page");
                    JSONObject startObj = jsonObject.getJSONObject("start");
                    JSONObject commonObj = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (pageObj != null || startObj != null) {
                        if (commonObj != null && commonObj.getString("mid") != null && ts != null) {
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    log.error("json 数据异常");
                }
            }
        });
    }
}
