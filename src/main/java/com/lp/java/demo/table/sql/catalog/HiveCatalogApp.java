package com.lp.java.demo.table.sql.catalog;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author li.pan
 * @title
 * @Date 2021/11/25
 */
public class HiveCatalogApp extends BaseTableEnv<String> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {
        String catalogName = "hiveCatalog01";
        String database = "test1";
        String hiveConfDir = "D:/gitProjects/flink_sql_tutorials/src/main/resources/conf";
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, database, hiveConfDir);
        tableStreamEnv.registerCatalog(catalogName,hiveCatalog);
        tableStreamEnv.useCatalog(catalogName);
        tableStreamEnv.useDatabase(database);
        tableStreamEnv.sqlQuery("select * from person limit 10").execute().print();
        tableStreamEnv.executeSql("insert into  person partition(dt='2021-07-20') values('ls',20)");
    }
}
