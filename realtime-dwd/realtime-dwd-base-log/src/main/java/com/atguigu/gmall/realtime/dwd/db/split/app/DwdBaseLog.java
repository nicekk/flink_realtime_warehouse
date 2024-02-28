package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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

        // 3. 拆分不同类型的用户行为日志
        // 启动日志：启动信息  + 报错信息
        // 页面日志：页面  曝光  动作 + 报错信息

        OutputTag<String> startTag = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> errTag = new OutputTag<>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<>("action", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, errTag, startTag, displayTag, actionTag);

        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errStream = pageStream.getSideOutput(errTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);

//        pageStream.print("page");
//        startStream.print("start");
//        errStream.print("err");
//        displayStream.print("display");
//        actionStream.print("action");

        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

    }

    private static SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> errTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                // 根据数据的不同，拆分到不同的侧输出流
                JSONObject errJson = jsonObject.getJSONObject("err");
                if (errJson != null) {
                    // 当前存在报错信息
                    context.output(errTag, errJson.toJSONString());
                    jsonObject.remove("err");
                }

                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject start = jsonObject.getJSONObject("start");
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");

                if (start != null) {
                    // 当前是启动日志，表示的就是本身，不需要额外处理了
                    context.output(startTag, jsonObject.toJSONString());
                } else if (page != null) {
                    JSONArray displays = jsonObject.getJSONArray("displays");

                    if (displays != null) {
                        // 曝光信息本身，加上公共信息，页面信息，和时间戳
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            context.output(displayTag, display.toJSONString());
                        }

                        jsonObject.remove("displays");
                    }


                    // 动作信息，加上公共信息，页面信息，和时间戳
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            context.output(actionTag, action.toJSONString());
                        }

                        jsonObject.remove("actions");
                    }


                    // 到这里只有 page 信息了，写出到主流
                    collector.collect(jsonObject.toJSONString());

                } else {

                }


            }
        });
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
