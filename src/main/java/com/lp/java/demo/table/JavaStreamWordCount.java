package com.lp.java.demo.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * <p/>
 * <li>title: Table APi</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/17 12:59 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 流环境执行Table API wordCount程序</li>
 */
public class JavaStreamWordCount {

	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings
				.newInstance()
				.useOldPlanner()
				.inStreamingMode()
				.build();
		StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

		String path = JavaStreamWordCount.class.getClassLoader().getResource("words.txt").getPath();
		fsTableEnv.connect(new FileSystem().path(path))
			.withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
			.withSchema(new Schema().field("word", Types.STRING))
			.inAppendMode()
			.registerTableSource("fileSource");

		Table result = fsTableEnv.scan("fileSource")
			.groupBy("word")
			.select("word, count(1) as count");

		fsTableEnv.toRetractStream(result, Row.class).print();
		fsTableEnv.execute(JavaStreamWordCount.class.getCanonicalName());
	}
}
