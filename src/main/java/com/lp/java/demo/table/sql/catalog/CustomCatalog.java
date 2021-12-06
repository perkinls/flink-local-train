package com.lp.java.demo.table.sql.catalog;

import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.table.api.Table;

/**
 * @author li.pan
 * @title 自定义catalog
 * @Date 2021/12/6
 */
public class CustomCatalog extends BaseTableEnv<Object> {
    public void customCatalog() {
        tableOldBatchEnv.useCatalog("custom_catalog");
        tableOldBatchEnv.useDatabase("custom_database");

//        tableStreamEnv.useCatalog("custom_catalog");
//        tableStreamEnv.useDatabase("custom_database");
//
//        tableBlinkBatchEnv.useCatalog("custom_database");
//        tableBlinkBatchEnv.useDatabase("custom_database");

        Table table = tableOldBatchEnv.fromDataSet(null);

        // register the view named 'exampleView' in the catalog named 'custom_catalog'
        // in the database named 'custom_database'
        tableOldBatchEnv.createTemporaryView("exampleView", table);

        // register the view named 'exampleView' in the catalog named 'custom_catalog'
        // in the database named 'other_database'
        tableOldBatchEnv.createTemporaryView("other_database.exampleView", table);

        // register the view named 'example.View' in the catalog named 'custom_catalog'
        // in the database named 'custom_database'
        tableOldBatchEnv.createTemporaryView("`example.View`", table);

        // register the view named 'exampleView' in the catalog named 'other_catalog'
        // in the database named 'other_database'
        tableOldBatchEnv.createTemporaryView("other_catalog.other_database.exampleView", table);

    }
}
