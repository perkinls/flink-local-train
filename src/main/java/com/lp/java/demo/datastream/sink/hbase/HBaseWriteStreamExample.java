

package com.lp.java.demo.datastream.sink.hbase;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * <p/>
 * <li>title: flink Sink</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/16 1:32 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: Flink中DataStream写入Hbase
 * </li>
 */
public class HBaseWriteStreamExample {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		// data stream with random numbers
		DataStream<String> dataStream = env.addSource(new SourceFunction<String>() {
			private static final long serialVersionUID = 1L;

			private volatile boolean isRunning = true;

			@Override
			public void run(SourceContext<String> out) throws Exception {
				while (isRunning) {
					out.collect(String.valueOf(Math.floor(Math.random() * 100)));
				}

			}

			@Override
			public void cancel() {
				isRunning = false;
			}
		});
		dataStream.writeUsingOutputFormat(new HBaseOutputFormat());

		env.execute(HBaseWriteStreamExample.class.getCanonicalName());
	}

	/**
	 * This class implements an OutputFormat for HBase.
	 */
	private static class HBaseOutputFormat implements OutputFormat<String> {

		private org.apache.hadoop.conf.Configuration conf = null;
		private HTable table = null;
		private String taskNumber = null;
		private int rowNumber = 0;

		private static final long serialVersionUID = 1L;

		@Override
		public void configure(Configuration parameters) {
			conf = HBaseConfiguration.create();
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			table = new HTable(conf, "flinkExample");
			this.taskNumber = String.valueOf(taskNumber);
		}

		@Override
		public void writeRecord(String record) throws IOException {
			Put put = new Put(Bytes.toBytes(taskNumber + rowNumber));
			put.add(Bytes.toBytes("entry"), Bytes.toBytes("entry"),
					Bytes.toBytes(rowNumber));
			rowNumber++;
			table.put(put);
		}

		@Override
		public void close() throws IOException {
			table.flushCommits();
			table.close();
		}

	}
}
