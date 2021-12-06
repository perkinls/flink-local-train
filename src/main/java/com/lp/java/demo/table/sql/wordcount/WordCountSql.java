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
 * @title sql word count程序
 * @Date 2021/12/2
 */
public class WordCountSql extends BaseTableEnv<WcPo> implements IBaseRunApp {

    private static final Logger log = LoggerFactory.getLogger(WordCountSql.class);

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

        // register the DataSet as table "WordCount"
        tableOldBatchEnv.createTemporaryView("WordCount", mockDataSetData(), $("word"), $("frequency"));

        // run a SQL query on the Table and retrieve the result as a new Table
        Table table = tableOldBatchEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");

        DataSet<WcPo> result = tableOldBatchEnv.toDataSet(table, WcPo.class);
        result.print();

    }


}
