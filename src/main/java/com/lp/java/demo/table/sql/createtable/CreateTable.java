package com.lp.java.demo.table.sql.createtable;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.WcPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.TableDescriptor;


/**
 * @author li.pan
 * @title 创建Table的几种方式
 * @Date 2021/12/6
 */
public class CreateTable extends BaseTableEnv<WcPo> implements IBaseRunApp {
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
        // No1.
        Table tableNo1 = tableOldBatchEnv.fromDataSet(mockDataSetData());
        tableOldBatchEnv.createTemporaryView("tableNo1", tableNo1);

        // No2. Using table descriptors
        //TableDescriptor sourceDescriptor = new TableDescriptor("datagen")
        //        .schema(Schema.SCHEMA
        //                .column("f0", DataTypes.STRING())
        //                .build())
        //        .option(DataGenOptions.ROWS_PER_SECOND, 100)
        //        .build();

        // No3.Using SQL DDL
        TableResult tableResult = tableOldBatchEnv.executeSql("CREATE [TEMPORARY] TABLE MyTable (...) WITH (...)");

    }
}
