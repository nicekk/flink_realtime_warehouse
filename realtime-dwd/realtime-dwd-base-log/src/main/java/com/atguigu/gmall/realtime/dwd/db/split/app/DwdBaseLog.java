package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.atguigu.gmall.realtime.common.base.BaseApp;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class DwdBaseLog extends BaseApp {

    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4,"dwd_base_log","topic_log");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

    }
}
