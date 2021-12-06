package com.lp.java.demo.table.sql.wordcount;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.WcPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author li.pan
 * @title table word count程序
 * @Date 2021/12/2
 */
public class WordCountTable extends BaseTableEnv<WcPo> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(WordCountTable.class);


    @Override
    protected DataSet<WcPo> mockDataSetData() {
        return batchEnv.fromElements(
                new WcPo("Hello", 1),
                new WcPo("Ciao", 1),
                new WcPo("Hello", 1)
        );
    }

    @Override
    public void doMain() throws Exception {
        Table table = tableOldBatchEnv.fromDataSet(mockDataSetData());
        Table filtered = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .filter($("frequency").isEqual(2));

        DataSet<WcPo> result = tableOldBatchEnv.toDataSet(filtered, WcPo.class);
        result.print();
    }


}
