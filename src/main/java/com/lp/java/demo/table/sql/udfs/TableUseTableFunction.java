package com.lp.java.demo.table.sql.udfs;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.WcPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


/**
 * @author li.pan
 * @title 表值函数
 * @Date 2021/12/15
 * 跟自定义标量函数一样，自定义表值函数的输入参数也可以是 0 到多个标量。但是跟标量函数只能返回一个值不同的是，它可以返回任意多行。
 * 官网地址: https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/functions/udfs/
 */
public class TableUseTableFunction extends BaseTableEnv<WcPo> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(TableUseTableFunction.class);

    @Override
    protected Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    protected DataStream<WcPo> mockDataStreamData() {
        return env.fromElements(
                new WcPo("He ll o", 1),
                new WcPo("Ci ao", 1),
                new WcPo("Hell o", 1)
        );
    }

    @Override
    public void doMain() throws Exception {
        tableStreamEnv.createTemporaryView("word_count_table", mockDataStreamData(), $("word"), $("frequency"), $("pt").proctime());
        Table wordCountTable = tableStreamEnv.from("word_count_table");

        //  ================== Table中的两种使用方式
        // 表值函数是通过 .joinLateral(...) 或者 .leftOuterJoinLateral(...) 来使用的

        // No.1 在 Table API 里不经注册直接“内联”调用函数
        log.info(" Table Api 内联方式使用表值函数 ");
        Table callRes1 = wordCountTable
                .joinLateral(call(SplitFunction.class, $("word")).as("new_word", "new_length"))
                .select($("word"), $("new_word"), $("new_length"), $("frequency"));

        Table callRes2 = wordCountTable
                .leftOuterJoinLateral(call(SplitFunction.class, $("word")).as("new_word", "new_length"))
                .select($("word"), $("new_word"), $("new_length"), $("frequency"));

        tableStreamEnv.toAppendStream(callRes1, Row.class).print("callRes1");
        tableStreamEnv.toAppendStream(callRes2, Row.class).print("callRes2");

        // No.2 在 Table API 注册函数调用函数
        log.info(" Table Api 内联方式使用表值函数 ");
        tableStreamEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        Table callRes3 = wordCountTable
                .joinLateral(call("SplitFunction", $("word")).as("new_word", "new_length"))
                .select($("word"), $("new_word"), $("new_length"), $("frequency"));

        Table callRes4 = wordCountTable
                .leftOuterJoinLateral(call("SplitFunction", $("word")).as("new_word", "new_length"))
                .select($("word"), $("new_word"), $("new_length"), $("frequency"));

        tableStreamEnv.toAppendStream(callRes3, Row.class).print("callRes3");
        tableStreamEnv.toAppendStream(callRes4, Row.class).print("callRes4");

        //  ================== SQL使用方式
        // 在 SQL 里调用注册好的函数
        tableStreamEnv.sqlQuery(
                "SELECT word, new_word, new_length, frequency " +
                        "FROM word_count_table " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(word)) AS T(new_word, frequency) ON TRUE");

        env.execute();
    }


    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>")) // 指定 方法返回数据类型
    public static class SplitFunction extends TableFunction<Row> {
        private static final long serialVersionUID = -8710366233273321111L;

        public void eval(String str) {
            for (String s : str.split(" ")) {
                // use collect(...) to emit a row
                collect(Row.of(s, s.length()));
            }
        }
    }
}
