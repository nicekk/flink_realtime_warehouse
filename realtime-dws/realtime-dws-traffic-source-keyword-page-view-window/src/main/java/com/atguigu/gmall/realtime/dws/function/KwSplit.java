package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.dws.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class KwSplit extends TableFunction<Row> {

    public void eval(String keywords) {
        for (String s : IkUtil.split(keywords)) {
            collect(Row.of(s));
        }
    }
}
