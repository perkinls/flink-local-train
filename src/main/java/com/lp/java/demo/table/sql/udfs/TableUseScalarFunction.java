package com.lp.java.demo.table.sql.udfs;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.WcPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author li.pan
 * @title 自定义标量函数
 * @Date 2021/12/14
 * 自定义标量函数可以把 0 到多个标量值映射成 1 个标量值
 * 官网地址: https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/functions/udfs/
 */
public class TableUseScalarFunction extends BaseTableEnv<WcPo> implements IBaseRunApp {

    private static final Logger log= LoggerFactory.getLogger(TableUseScalarFunction.class);

    @Override
    protected DataStream<WcPo> mockDataStreamData() {
        return env.fromElements(
                new WcPo("Hello", 1),
                new WcPo("Ciao", 1),
                new WcPo("Hello", 1)
        );
    }

    @Override
    public void doMain() throws Exception {
        tableStreamEnv.createTemporaryView("word_count_table", mockDataStreamData(), $("word"), $("frequency"), $("pt").proctime());
        Table wordCountTable = tableStreamEnv.from("word_count_table");

        //  ================== Table中的两种使用方式
        // 使用 $("*") 作为函数的参数，如果 MyTable 有 3 列 (a, b, c)，它们都将会被传给 MyConcatFunction。

        // No.1 在 Table API 里不经注册直接“内联”调用函数
        log.info(" Table Api 内联方式使用标量函数 ");
        Table callRes1 = wordCountTable.select(call(SubstringFunction.class, $("word"), 0, 3));
        tableStreamEnv.toAppendStream(callRes1, Row.class).print("callRes1");
        // No.2 注册函数
        log.info(" Table Api 注册函数方式使用标量函数 ");
        tableStreamEnv.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);
        Table callRes2 =wordCountTable.select(call("SubstringFunction", $("word"), 0,3));
        tableStreamEnv.toAppendStream(callRes2, Row.class).print("callRes2");

        //  ================== SQL使用方式
        // 在 SQL 里调用注册好的函数
        Table sqlRes=tableStreamEnv.sqlQuery("SELECT SubstringFunction(word, 0, 3) FROM word_count_table");
        tableStreamEnv.toAppendStream(sqlRes, Row.class).print("sqlRes");

        env.execute();
    }


    /**
     * 该类必须声明为 public ，而不是 abstract ，并且可以被全局访问。不允许使用非静态内部类或匿名类。
     */
    public static class SubstringFunction extends ScalarFunction {
        private static final long serialVersionUID = -679956643502214158L;

        // tips: 为了将自定义函数存储在持久化的 catalog 中，该类必须具有默认构造器

        // 这些方法必须声明为 public
        public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, end);
        }
    }
}
